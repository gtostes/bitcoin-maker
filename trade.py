#!/usr/bin/env python3
"""
POLYMARKET MARKET MAKER

Estratégia:
- Coloca 2 quotes (bid e ask) baseadas no ref_price
- bid = min(ref_price - 0.01 - 0.001*pos, best_bid)
- ask = max(ref_price + 0.01 - 0.001*pos, best_ask)
- Posição máxima: 25
- Quando há fill: recalcula e recoloca quotes
- Se atingiu inventário limite: coloca apenas um lado
"""

import os
import time
import asyncio
import re
from decimal import Decimal, ROUND_HALF_UP

import zmq
import zmq.asyncio

from py_clob_client.client import ClobClient
from py_clob_client.clob_types import OrderArgs, OrderType
from py_clob_client.order_builder.constants import BUY, SELL
from py_clob_client.constants import POLYGON
from dotenv import load_dotenv

from utils import get_markets

load_dotenv()


# ============================================================
# CONFIGURAÇÕES
# ============================================================

# IPC Endpoints
IPC_PRICE_PRED = "ipc:///tmp/btc_price_pred.ipc"  # Recebe ref_price (pred_model2)
IPC_MARKET = "ipc:///tmp/polymarket_market.ipc"   # Recebe bid/ask
IPC_PRICE15 = "ipc:///tmp/btc_price_15.ipc"       # Recebe price_15 (detecção de período)
IPC_FILLS = "ipc:///tmp/polymarket_fills.ipc"     # Recebe fills do User Channel

# Parâmetros de trading
TRADE_SIZE = 5           # Tamanho de cada quote
MAX_POSITION = 25        # Posição máxima (YES - NO)
SPREAD_BASE = 0.02       # Spread base (1 cent cada lado)
SKEW_FACTOR = 0.001      # Fator de skew por unidade de posição

# Intervalos de tempo (em minutos no período de 15min)
NO_TRADE_START = 0.5     # Não opera nos primeiros 30s
NO_TRADE_END = 14.5      # Não opera depois de 14:30

# Tick size do Polymarket
TICK_SIZE = Decimal("0.01")


# ============================================================
# HELPERS
# ============================================================

def round_price(price: float) -> float:
    """Arredonda preço para o tick size (0.01) - arredondamento padrão"""
    d = Decimal(str(price)).quantize(TICK_SIZE, rounding=ROUND_HALF_UP)
    return float(d)


def clamp(value: float, min_val: float, max_val: float) -> float:
    """Limita valor entre min e max"""
    return max(min_val, min(max_val, value))


def authenticate_polymarket():
    """Autentica no Polymarket"""
    host = "https://clob.polymarket.com"
    key = os.getenv("PK")
    proxy_address = os.getenv("BROWSER_ADDRESS")
    
    client = ClobClient(
        host, 
        key=key, 
        chain_id=POLYGON, 
        signature_type=1, 
        funder=proxy_address
    )
    client.set_api_creds(client.create_or_derive_api_creds())
    return client


def get_current_token_ids():
    """Busca os token IDs do mercado BTC 15min atual."""
    try:
        time_market = time.time() // 900
        id_time = 900 * int(time_market)
        
        url = f'https://polymarket.com/event/btc-updown-15m-{id_time}'
        markets = get_markets(url)
        
        if not markets:
            print(f"❌ Nenhum mercado encontrado para {url}")
            return None, None
        
        ids = re.findall(r'"(\d+)"', markets[0]['clobTokenIds'])
        
        if len(ids) >= 2:
            token_id_yes = ids[0]
            token_id_no = ids[1]
            print(f"✅ Token IDs: YES={token_id_yes[:20]}... | NO={token_id_no[:20]}...")
            return token_id_yes, token_id_no
        else:
            print(f"❌ Token IDs insuficientes: {ids}")
            return None, None
            
    except Exception as e:
        print(f"❌ Erro ao buscar token IDs: {e}")
        return None, None


# ============================================================
# MARKET MAKER
# ============================================================

class MarketMaker:
    def __init__(self, client: ClobClient, loop: asyncio.AbstractEventLoop):
        self.client = client
        self.loop = loop
        
        # Token IDs
        self.token_id_yes = None
        self.token_id_no = None
        
        # Estado do mercado
        self.best_bid = None   # Melhor bid do mercado (para YES)
        self.best_ask = None   # Melhor ask do mercado (para YES)
        self.ref_price = None  # Preço justo calculado
        self.price_15 = None   # Preço de referência do período
        
        # Posição
        self.pos_yes = 0.0
        self.pos_no = 0.0
        
        # Ordens ativas (order_id -> info)
        self.active_orders = {}
        
        # Preços das quotes atuais (para evitar cancelar desnecessariamente)
        self.current_bid_price = None  # Preço do BID YES atual
        self.current_ask_price = None  # Preço do BID NO atual (equivalente ao ask)
        
        # PnL tracking
        self.pnl = 0.0
        self.total_traded = 0.0
        
        # Estado de controle
        self.period_start_time = None
        self.is_updating_quotes = False
        self.quotes_need_update = False
        
        # Debounce
        self.last_quote_update = 0
        self.quote_update_interval = 0.5  # Mínimo 500ms entre updates
    
    @property
    def position(self) -> float:
        """Posição líquida (positivo = long YES, negativo = long NO)"""
        return self.pos_yes - self.pos_no
    
    def update_token_ids(self) -> bool:
        """Atualiza os token IDs para o mercado atual"""
        token_id_yes, token_id_no = get_current_token_ids()
        if token_id_yes and token_id_no:
            self.token_id_yes = token_id_yes
            self.token_id_no = token_id_no
            return True
        return False
    
    def reset_period(self):
        """Reset para novo período de 15min"""
        if self.price_15 is not None:
            print(f"📊 Período encerrado | Posição: YES={self.pos_yes:.2f} NO={self.pos_no:.2f}")
        
        self.cancel_all_orders()
        
        self.pos_yes = 0.0
        self.pos_no = 0.0
        self.active_orders = {}
        self.current_bid_price = None
        self.current_ask_price = None
        self.period_start_time = time.time()
        self.is_updating_quotes = False
        
        print("🔄 Reset para novo período")
    
    # ========== ORDER MANAGEMENT ==========
    
    def cancel_all_orders(self):
        """Cancela todas as ordens ativas"""
        if not self.active_orders:
            return
        
        try:
            order_ids = list(self.active_orders.keys())
            if order_ids:
                self.client.cancel_orders(order_ids)
                print(f"🗑️ Canceladas {len(order_ids)} ordens")
            self.active_orders = {}
        except Exception as e:
            print(f"❌ Erro ao cancelar ordens: {e}")
            self.active_orders = {}
    
    def place_order(self, token_id: str, price: float, size: float, side: str, token_name: str) -> str | None:
        """Coloca uma ordem GTC (maker)"""
        try:
            price = round_price(price)
            if price <= 0 or price >= 1:
                return None
            
            order_args = OrderArgs(
                price=price,
                size=size,
                side=side,
                token_id=token_id,
            )
            
            signed_order = self.client.create_order(order_args)
            resp = self.client.post_order(signed_order, orderType=OrderType.GTC)
            
            if resp and 'orderID' in resp:
                order_id = resp['orderID']
                self.active_orders[order_id] = {
                    'side': side,
                    'token': token_name,
                    'price': price,
                    'size': size
                }
                return order_id
            return None
            
        except Exception as e:
            print(f"❌ Erro ao colocar ordem: {e}")
            return None
    
    def calculate_quotes(self) -> tuple[float | None, float | None]:
        """
        Calcula os preços de bid e ask baseado no ref_price e posição.
        
        bid = min(ref_price - 0.01 - 0.001*pos, best_bid)
        ask = max(ref_price + 0.01 - 0.001*pos, best_ask)
        """
        if self.ref_price is None:
            return None, None
        
        pos = self.position
        
        # Cálculo base com skew
        my_bid = self.ref_price - SPREAD_BASE - SKEW_FACTOR * pos
        my_ask = self.ref_price + SPREAD_BASE - SKEW_FACTOR * pos
        
        # Aplica limites do mercado
        if self.best_bid is not None:
            my_bid = min(my_bid, self.best_bid)
        if self.best_ask is not None:
            my_ask = max(my_ask, self.best_ask)
        
        # Arredonda para tick size
        my_bid = round_price(my_bid)
        my_ask = round_price(my_ask)
        
        # Garante limites
        my_bid = clamp(my_bid, 0.01, 0.99)
        my_ask = clamp(my_ask, 0.01, 0.99)
        
        # Garante spread mínimo
        if my_ask <= my_bid:
            my_ask = my_bid + 0.01
        
        return my_bid, my_ask
    
    async def update_quotes(self):
        """Atualiza as quotes no mercado"""
        if self.is_updating_quotes:
            self.quotes_need_update = True
            return
        
        self.is_updating_quotes = True
        
        try:
            if not self._can_trade():
                if self.active_orders:
                    await asyncio.to_thread(self.cancel_all_orders)
                    self.current_bid_price = None
                    self.current_ask_price = None
                return
            
            if self.ref_price is None:
                return
            if self.token_id_yes is None or self.token_id_no is None:
                return
            
            # Debounce
            now = time.time()
            if now - self.last_quote_update < self.quote_update_interval:
                self.quotes_need_update = True
                return
            self.last_quote_update = now
            
            # Calcula novos preços
            my_bid, my_ask = self.calculate_quotes()
            
            if my_bid is None or my_ask is None:
                return
            
            no_price = round_price(1 - my_ask)
            pos = self.position
            
            # Verifica se os preços mudaram
            bid_changed = (self.current_bid_price != my_bid)
            ask_changed = (self.current_ask_price != no_price)
            
            # Se nenhum preço mudou, não faz nada
            if not bid_changed and not ask_changed:
                return
            
            # Cancela ordens antigas apenas se algum preço mudou
            if self.active_orders:
                await asyncio.to_thread(self.cancel_all_orders)
            
            orders_placed = []
            
            # BID (comprar YES) - só se não atingiu limite positivo
            if pos < MAX_POSITION:
                order_id = await asyncio.to_thread(
                    self.place_order, 
                    self.token_id_yes, 
                    my_bid, 
                    TRADE_SIZE, 
                    BUY, 
                    'YES'
                )
                if order_id:
                    self.current_bid_price = my_bid
                    orders_placed.append(f"BID YES @ {my_bid:.2f}")
            
            # ASK (comprar NO a preço 1-ask) - só se não atingiu limite negativo
            # Vender YES @ ask é equivalente a comprar NO @ (1-ask)
            if pos > -MAX_POSITION:
                order_id = await asyncio.to_thread(
                    self.place_order, 
                    self.token_id_no, 
                    no_price, 
                    TRADE_SIZE, 
                    BUY, 
                    'NO'
                )
                if order_id:
                    self.current_ask_price = no_price
                    orders_placed.append(f"BID NO @ {no_price:.2f} (ask={my_ask:.2f})")
            
            if orders_placed:
                print(f"📝 Quotes: {' | '.join(orders_placed)} | pos={pos:.1f} | ref={self.ref_price:.4f}")
        
        except Exception as e:
            print(f"❌ Erro ao atualizar quotes: {e}")
        
        finally:
            self.is_updating_quotes = False
            
            if self.quotes_need_update:
                self.quotes_need_update = False
                asyncio.create_task(self.update_quotes())
    
    def _can_trade(self) -> bool:
        """Verifica se pode operar no momento atual"""
        # Não opera no primeiro período (period_start_time só é setado no reset_period)
        if self.period_start_time is None:
            return False
        
        tm_seconds = time.time() % 900
        t = tm_seconds / 60
        return NO_TRADE_START <= t <= NO_TRADE_END
    
    # ========== EVENT HANDLERS ==========
    
    def on_bid_ask(self, data: dict):
        """Processa atualização de bid/ask do mercado"""
        new_bid = data.get("best_bid")
        new_ask = data.get("best_ask")
        
        if new_bid is not None:
            self.best_bid = float(new_bid)
        if new_ask is not None:
            self.best_ask = float(new_ask)
    
    def on_ref_price(self, data: dict):
        """Processa atualização do preço de referência"""
        if "pred_model2" in data:
            pred = data["pred_model2"]
            if pred is not None:
                old_ref = self.ref_price
                self.ref_price = float(pred)
                
                # DESABILITADO: Requote automático quando preço muda
                # Se mudou significativamente, atualiza quotes
                # if old_ref is None or abs(self.ref_price - old_ref) > 0.005:
                #     asyncio.create_task(self.update_quotes())
                
                # Apenas coloca quotes iniciais se ainda não temos nenhuma
                if old_ref is None and self._can_trade():
                    asyncio.create_task(self.update_quotes())
    
    def on_price_15(self, new_price_15: float):
        """Processa atualização do price_15 (detecta mudança de período)"""
        if self.price_15 is None:
            # Primeiro período - apenas salva o preço, não inicia trading
            # period_start_time permanece None, então _can_trade() retorna False
            self.price_15 = new_price_15
            print(f"📊 Primeiro período detectado (price_15={new_price_15}). Aguardando próximo período para operar.")
            return
        
        if new_price_15 != self.price_15:
            old_price = self.price_15
            self.price_15 = new_price_15
            
            if new_price_15 > old_price:
                period_pnl = self.pos_yes
                print(f"📈 Preço subiu! YES ganha. PnL período: +{period_pnl:.2f}")
            else:
                period_pnl = self.pos_no
                print(f"📉 Preço caiu! NO ganha. PnL período: +{period_pnl:.2f}")
            
            self.pnl += period_pnl
            print(f"💰 PnL total: {self.pnl:.2f}")
            
            self.reset_period()
            print("\n🔄 Novo período! Renovando token IDs...")
            self.update_token_ids()
            
            asyncio.get_event_loop().call_later(2.0, lambda: asyncio.create_task(self.update_quotes()))
    
    def on_fill(self, fill_data: dict):
        """Processa fill recebido do User Channel."""
        side = fill_data.get('side')
        outcome = fill_data.get('outcome')
        size = float(fill_data.get('size', 0))
        price = float(fill_data.get('price', 0))
        is_maker = fill_data.get('is_maker', False)
        
        if size <= 0:
            return
        
        outcome_upper = outcome.upper() if outcome else ''
        
        # Atualiza posição e PnL
        # Quando compro, gasto dinheiro (pnl diminui)
        # Quando vendo, recebo dinheiro (pnl aumenta)
        cost = price * size
        
        if side == 'BUY':
            self.pnl -= cost  # Gastei dinheiro comprando
            if outcome_upper == 'YES':
                self.pos_yes += size
            elif outcome_upper == 'NO':
                self.pos_no += size
        elif side == 'SELL':
            self.pnl += cost  # Recebi dinheiro vendendo
            if outcome_upper == 'YES':
                self.pos_yes -= size
            elif outcome_upper == 'NO':
                self.pos_no -= size
        
        self.total_traded += size
        
        # Calcula PnL total (realizado + não realizado)
        mid_price = self.ref_price if self.ref_price else 0.5
        unrealized = self.pos_yes * mid_price + self.pos_no * (1 - mid_price)
        total_pnl = self.pnl + unrealized
        
        maker_str = "MAKER" if is_maker else "TAKER"
        print(f"💰 FILL [{maker_str}]: {side} {size} {outcome} @ {price:.4f}")
        print(f"📊 Posição: YES={self.pos_yes:.2f} | NO={self.pos_no:.2f} | PnL Total={total_pnl:.4f}")
        
        # Reseta preços para forçar recálculo (posição mudou, precisa de novos preços)
        self.current_bid_price = None
        self.current_ask_price = None
        
        # Recoloca quotes após fill
        asyncio.create_task(self.update_quotes())


# ============================================================
# IPC READERS
# ============================================================

async def read_ref_price(zmq_context, maker: MarketMaker):
    """Lê ref_price (pred_model2) do process-bitcoin-price.py"""
    socket = zmq_context.socket(zmq.PULL)
    socket.connect(IPC_PRICE_PRED)
    print(f"🔌 Conectado ao IPC ref_price: {IPC_PRICE_PRED}")
    
    first_msg = True
    while True:
        try:
            data = await socket.recv_json()
            
            if first_msg:
                pred = data.get('pred_model2')
                pred_str = f"{pred:.4f}" if pred is not None else "N/A"
                print(f"✅ Primeiro ref_price: {pred_str}")
                first_msg = False
            
            maker.on_ref_price(data)
        
        except zmq.ZMQError as e:
            if e.errno == zmq.ETERM:
                break
            print(f"❌ Erro ZMQ ref_price: {e}")
            await asyncio.sleep(0.1)
        except asyncio.CancelledError:
            break
        except Exception as e:
            print(f"❌ Erro ref_price: {e}")
            await asyncio.sleep(0.1)
    
    socket.close()


async def read_bid_ask(zmq_context, maker: MarketMaker):
    """Lê bid/ask do market-user-stream.py"""
    socket = zmq_context.socket(zmq.PULL)
    socket.connect(IPC_MARKET)
    print(f"🔌 Conectado ao IPC mercado: {IPC_MARKET}")
    
    first_msg = True
    
    while True:
        try:
            data = await socket.recv_json()
            
            # Drena fila para pegar o mais recente
            while True:
                try:
                    data = await asyncio.wait_for(socket.recv_json(), timeout=0.001)
                except asyncio.TimeoutError:
                    break
            
            if first_msg and "best_bid" in data:
                print(f"✅ Primeiro bid/ask: bid={data['best_bid']} | ask={data['best_ask']}")
                first_msg = False
            
            maker.on_bid_ask(data)
        
        except zmq.ZMQError as e:
            if e.errno == zmq.ETERM:
                break
            print(f"❌ Erro ZMQ bid/ask: {e}")
            await asyncio.sleep(0.1)
        except asyncio.CancelledError:
            break
        except Exception as e:
            print(f"❌ Erro bid/ask: {e}")
            await asyncio.sleep(0.1)
    
    socket.close()


async def read_price_15(zmq_context, maker: MarketMaker):
    """Lê price_15 (detecta mudança de período)"""
    socket = zmq_context.socket(zmq.SUB)
    socket.connect(IPC_PRICE15)
    socket.setsockopt_string(zmq.SUBSCRIBE, "")
    print(f"🔌 Conectado ao IPC price_15: {IPC_PRICE15}")
    
    first_msg = True
    while True:
        try:
            data = await socket.recv_json()
            
            if "price_15" in data:
                if first_msg:
                    print(f"✅ Primeiro price_15: {data['price_15']}")
                    first_msg = False
                maker.on_price_15(data["price_15"])
        
        except zmq.ZMQError as e:
            if e.errno == zmq.ETERM:
                break
            print(f"❌ Erro ZMQ price_15: {e}")
            await asyncio.sleep(0.1)
        except asyncio.CancelledError:
            break
        except Exception as e:
            print(f"❌ Erro price_15: {e}")
            await asyncio.sleep(0.1)
    
    socket.close()


async def read_fills(zmq_context, maker: MarketMaker):
    """Lê fills do market-user-stream.py"""
    socket = zmq_context.socket(zmq.PULL)
    socket.connect(IPC_FILLS)
    print(f"🔌 Conectado ao IPC fills: {IPC_FILLS}")
    
    first_msg = True
    while True:
        try:
            data = await socket.recv_json()
            
            if first_msg:
                print(f"✅ Primeiro fill recebido: {data}")
                first_msg = False
            
            maker.on_fill(data)
        
        except zmq.ZMQError as e:
            if e.errno == zmq.ETERM:
                break
            print(f"❌ Erro ZMQ fills: {e}")
            await asyncio.sleep(0.1)
        except asyncio.CancelledError:
            break
        except Exception as e:
            print(f"❌ Erro fills: {e}")
            await asyncio.sleep(0.1)
    
    socket.close()


async def status_printer(maker: MarketMaker):
    """Imprime status periodicamente"""
    while True:
        await asyncio.sleep(5)
        
        if maker.ref_price is not None:
            tm_seconds = time.time() % 900
            t = tm_seconds / 60
            
            my_bid, my_ask = maker.calculate_quotes()
            bid_str = f"{my_bid:.2f}" if my_bid else "N/A"
            ask_str = f"{my_ask:.2f}" if my_ask else "N/A"
            
            can_trade = "✅" if maker._can_trade() else "❌"
            
            # PnL total = realizado + não realizado
            mid_price = maker.ref_price
            unrealized = maker.pos_yes * mid_price + maker.pos_no * (1 - mid_price)
            total_pnl = maker.pnl + unrealized
            
            print(
                f"📊 ref={maker.ref_price:.4f} | "
                f"quotes: {bid_str}/{ask_str} | "
                f"bid/ask: {maker.best_bid:.2f}/{maker.best_ask:.2f} | "
                f"pos: {maker.pos_yes:.1f}Y/{maker.pos_no:.1f}N | "
                f"pnl={total_pnl:.4f} | "
                f"t={t:.1f}min {can_trade}"
            )


# ============================================================
# MAIN
# ============================================================

async def main():
    """Loop principal do Market Maker"""
    print("=" * 60)
    print("POLYMARKET MARKET MAKER")
    print("=" * 60)
    print(f"📋 Estratégia:")
    print(f"   - bid = min(ref_price - {SPREAD_BASE} - {SKEW_FACTOR}*pos, best_bid)")
    print(f"   - ask = max(ref_price + {SPREAD_BASE} - {SKEW_FACTOR}*pos, best_ask)")
    print(f"   - Trade size: {TRADE_SIZE}")
    print(f"   - Max position: ±{MAX_POSITION}")
    print(f"   - Horário: {NO_TRADE_START:.1f} - {NO_TRADE_END:.1f} min")
    print("=" * 60)
    
    print("\n🔐 Autenticando no Polymarket...")
    try:
        client = authenticate_polymarket()
        print("✅ Autenticação bem sucedida!")
    except Exception as e:
        print(f"❌ Erro na autenticação: {e}")
        return
    
    loop = asyncio.get_running_loop()
    maker = MarketMaker(client, loop)
    
    print("\n🔄 Buscando token IDs do mercado atual...")
    if not maker.update_token_ids():
        print("⚠️ Falha ao buscar token IDs iniciais (pode funcionar depois)")
    
    zmq_context = zmq.asyncio.Context()
    
    print("\n🚀 Iniciando Market Maker...")
    print(f"📡 IPC ref_price: {IPC_PRICE_PRED}")
    print(f"📡 IPC mercado: {IPC_MARKET}")
    print(f"📡 IPC price_15: {IPC_PRICE15}")
    print(f"📡 IPC fills: {IPC_FILLS}")
    print("\n⏳ Aguardando dados...\n")
    
    tasks = [
        asyncio.create_task(read_ref_price(zmq_context, maker)),
        asyncio.create_task(read_bid_ask(zmq_context, maker)),
        asyncio.create_task(read_price_15(zmq_context, maker)),
        asyncio.create_task(read_fills(zmq_context, maker)),
        asyncio.create_task(status_printer(maker)),
    ]
    
    try:
        await asyncio.gather(*tasks)
    except KeyboardInterrupt:
        print("\n👋 Encerrando...")
    finally:
        print("🗑️ Cancelando ordens pendentes...")
        maker.cancel_all_orders()
        
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        zmq_context.term()
        print("✅ Encerrado!")


if __name__ == "__main__":
    asyncio.run(main())
