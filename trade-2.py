#!/usr/bin/env python3
"""
POLYMARKET MARKET MAKER V2

Estratégia:
- Fiscaliza quotes a cada 0.2s (não reage a eventos)
- Compra pelo maior preço <= min(price_pred - 0.05 - 0.002*pos, best_bid)
- Vende pelo menor preço >= max(price_pred + 0.05 - 0.002*pos, best_ask)
- Cancela BID se:
  - price > price_pred - 0.04 - 0.002*pos (muito caro)
  - price < best_bid - 0.10 (muito longe do mercado)
- Cancela ASK se:
  - price < price_pred + 0.04 - 0.002*pos (muito barato)
  - price > best_ask + 0.10 (muito longe do mercado)
- Posição máxima: ±25, trade size: 5
- Não opera nos primeiros 30s e últimos 30s do período
"""

import os
import sys
import time
import asyncio
import re
from decimal import Decimal, ROUND_DOWN, ROUND_UP
from dataclasses import dataclass, field
from enum import Enum, auto

import zmq
import zmq.asyncio

from py_clob_client.client import ClobClient
from py_clob_client.clob_types import OrderArgs, OrderType
from py_clob_client.order_builder.constants import BUY, SELL
from py_clob_client.constants import POLYGON
from dotenv import load_dotenv

# Adiciona bitcoin-strategy ao path para importar utils
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'bitcoin-strategy'))
from utils import get_markets

load_dotenv()


# ============================================================
# CONFIGURAÇÕES
# ============================================================

# IPC Endpoints
IPC_PRICE_PRED = "ipc:///tmp/btc_price_pred.ipc"  # Recebe price_pred (pred_model2)
IPC_MARKET = "ipc:///tmp/polymarket_market.ipc"   # Recebe bid/ask
IPC_PRICE15 = "ipc:///tmp/btc_price_15.ipc"       # Recebe price_15 (detecção de período)
IPC_FILLS = "ipc:///tmp/polymarket_fills.ipc"     # Recebe fills do User Channel

# Parâmetros de trading
TRADE_SIZE = 5            # Tamanho de cada quote
MAX_POSITION = 25         # Posição máxima (YES - NO)
SPREAD_QUOTE = 0.05       # Spread para colocar quote (5 cents cada lado)
SPREAD_CANCEL = 0.04      # Spread para cancelar quote (4 cents cada lado)
SKEW_FACTOR = 0.002       # Fator de skew por unidade de posição
MAX_DISTANCE = 0.10       # Distância máxima do mercado para cancelar (10 cents)

# Intervalos de tempo
FISCALIZE_INTERVAL = 0.2  # Fiscaliza quotes a cada 200ms
NO_TRADE_START = 0.5      # Não opera nos primeiros 30s (0.5 min)
NO_TRADE_END = 14.5       # Não opera depois de 14:30 (14.5 min)

# Tick size do Polymarket
TICK_SIZE = Decimal("0.01")


# ============================================================
# HELPERS
# ============================================================

def round_price_down(price: float) -> float:
    """Arredonda preço para baixo (para bids)"""
    d = Decimal(str(price)).quantize(TICK_SIZE, rounding=ROUND_DOWN)
    return float(d)


def round_price_up(price: float) -> float:
    """Arredonda preço para cima (para asks)"""
    d = Decimal(str(price)).quantize(TICK_SIZE, rounding=ROUND_UP)
    return float(d)


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
# ORDER STATE
# ============================================================

class OrderState(Enum):
    """Estado de uma ordem"""
    NONE = auto()           # Sem ordem
    ACTIVE = auto()         # Ordem ativa no mercado
    CANCELLING = auto()     # Cancelando (aguardando confirmação)


@dataclass
class OrderInfo:
    """Informações de uma ordem"""
    order_id: str = None
    price: float = None
    size: float = None
    state: OrderState = OrderState.NONE


# ============================================================
# MARKET MAKER V2
# ============================================================

class MarketMakerV2:
    def __init__(self, client: ClobClient):
        self.client = client
        
        # Token IDs
        self.token_id_yes = None
        self.token_id_no = None
        
        # Estado do mercado (atualizado assincronamente)
        self.best_bid = None
        self.best_ask = None
        self.price_pred = None  # pred_model2
        self.price_15 = None
        
        # Posição
        self.pos_yes = 0.0
        self.pos_no = 0.0
        
        # Estado das ordens (BID = comprar YES, ASK = vender YES via comprar NO)
        self.bid_order = OrderInfo()
        self.ask_order = OrderInfo()
        
        # PnL tracking
        self.pnl = 0.0
        self.total_traded = 0.0
        
        # Estado de controle
        self.period_start_time = None
        self.first_period = True
        
        # Lock para operações de ordem (evita race conditions)
        self.order_lock = asyncio.Lock()
    
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
        
        # Cancela ordens pendentes
        self._cancel_all_orders_sync()
        
        # Reset estado
        self.pos_yes = 0.0
        self.pos_no = 0.0
        self.bid_order = OrderInfo()
        self.ask_order = OrderInfo()
        self.period_start_time = time.time()
        self.first_period = False
        
        print("🔄 Reset para novo período")
    
    def _can_trade(self) -> bool:
        """Verifica se pode operar no momento atual"""
        if self.first_period:
            return False
        if self.period_start_time is None:
            return False
        
        tm_seconds = time.time() % 900
        t = tm_seconds / 60  # tempo em minutos
        return NO_TRADE_START <= t <= NO_TRADE_END
    
    # ========== CÁLCULO DE PREÇOS ==========
    
    def calculate_bid_price(self) -> float | None:
        """
        Calcula preço do BID (comprar YES).
        bid = min(floor(price_pred - 0.05 - 0.002*pos), best_bid)
        """
        if self.price_pred is None:
            return None
        if self.best_bid is None:
            return None
        
        pos = self.position
        raw_price = self.price_pred - SPREAD_QUOTE - SKEW_FACTOR * pos
        model_bid = round_price_down(raw_price)
        
        # Limita pelo best_bid do mercado
        bid_price = min(model_bid, self.best_bid)
        
        if bid_price <= 0 or bid_price >= 1:
            return None
        
        return bid_price
    
    def calculate_ask_price(self) -> float | None:
        """
        Calcula preço do ASK (vender YES).
        ask = max(ceil(price_pred + 0.05 - 0.002*pos), best_ask)
        """
        if self.price_pred is None:
            return None
        if self.best_ask is None:
            return None
        
        pos = self.position
        raw_price = self.price_pred + SPREAD_QUOTE - SKEW_FACTOR * pos
        model_ask = round_price_up(raw_price)
        
        # Limita pelo best_ask do mercado
        ask_price = max(model_ask, self.best_ask)
        
        if ask_price <= 0 or ask_price >= 1:
            return None
        
        return ask_price
    
    def should_cancel_bid(self, current_price: float) -> bool:
        """
        Verifica se deve cancelar BID.
        Cancela se:
        1. price > price_pred - 0.04 - 0.002*pos (muito caro)
        2. price < best_bid - 0.10 (muito longe do mercado)
        """
        if self.price_pred is None:
            return True  # Sem preço de referência, cancela
        
        pos = self.position
        
        # Condição 1: preço muito alto (próximo demais do fair value)
        threshold_high = self.price_pred - SPREAD_CANCEL - SKEW_FACTOR * pos
        if current_price > threshold_high:
            return True
        
        # Condição 2: preço muito longe do mercado
        if self.best_bid is not None and current_price < self.best_bid - MAX_DISTANCE:
            return True
        
        return False
    
    def should_cancel_ask(self, current_price: float) -> bool:
        """
        Verifica se deve cancelar ASK.
        Cancela se:
        1. price < price_pred + 0.04 - 0.002*pos (muito barato)
        2. price > best_ask + 0.10 (muito longe do mercado)
        """
        if self.price_pred is None:
            return True  # Sem preço de referência, cancela
        
        pos = self.position
        
        # Condição 1: preço muito baixo (próximo demais do fair value)
        threshold_low = self.price_pred + SPREAD_CANCEL - SKEW_FACTOR * pos
        if current_price < threshold_low:
            return True
        
        # Condição 2: preço muito longe do mercado
        if self.best_ask is not None and current_price > self.best_ask + MAX_DISTANCE:
            return True
        
        return False
    
    # ========== OPERAÇÕES DE ORDEM (SÍNCRONAS) ==========
    
    def _place_order_sync(self, token_id: str, price: float, size: float, side: str) -> str | None:
        """Coloca uma ordem GTC (maker) de forma síncrona"""
        try:
            order_args = OrderArgs(
                price=price,
                size=size,
                side=side,
                token_id=token_id,
            )
            
            signed_order = self.client.create_order(order_args)
            resp = self.client.post_order(signed_order, orderType=OrderType.GTC)
            
            if resp and 'orderID' in resp:
                return resp['orderID']
            return None
            
        except Exception as e:
            print(f"❌ Erro ao colocar ordem: {e}")
            return None
    
    def _cancel_order_sync(self, order_id: str) -> bool:
        """Cancela uma ordem de forma síncrona"""
        try:
            self.client.cancel(order_id)
            return True
        except Exception as e:
            # Pode falhar se já foi executada
            print(f"⚠️ Erro ao cancelar ordem {order_id[:8]}...: {e}")
            return False
    
    def _cancel_all_orders_sync(self):
        """Cancela todas as ordens ativas"""
        order_ids = []
        if self.bid_order.state == OrderState.ACTIVE and self.bid_order.order_id:
            order_ids.append(self.bid_order.order_id)
        if self.ask_order.state == OrderState.ACTIVE and self.ask_order.order_id:
            order_ids.append(self.ask_order.order_id)
        
        if order_ids:
            try:
                self.client.cancel_orders(order_ids)
                print(f"🗑️ Canceladas {len(order_ids)} ordens")
            except Exception as e:
                print(f"❌ Erro ao cancelar ordens: {e}")
    
    # ========== LOOP PRINCIPAL DE FISCALIZAÇÃO ==========
    
    async def fiscalize_loop(self):
        """
        Loop principal que fiscaliza as quotes a cada 0.2s.
        Este é o ÚNICO lugar que decide colocar/cancelar ordens.
        """
        print("🔍 Iniciando loop de fiscalização...")
        
        while True:
            try:
                await asyncio.sleep(FISCALIZE_INTERVAL)
                await self._fiscalize_quotes()
            except asyncio.CancelledError:
                break
            except Exception as e:
                print(f"❌ Erro no loop de fiscalização: {e}")
                await asyncio.sleep(0.5)
    
    async def _fiscalize_quotes(self):
        """
        Fiscaliza e ajusta as quotes.
        Executado a cada 0.2s.
        """
        # Verifica se pode operar
        if not self._can_trade():
            # Fora do horário: cancela tudo
            if self.bid_order.state == OrderState.ACTIVE or self.ask_order.state == OrderState.ACTIVE:
                async with self.order_lock:
                    await self._cancel_all_quotes()
            return
        
        # Verifica dados necessários
        if self.price_pred is None:
            return
        if self.token_id_yes is None or self.token_id_no is None:
            return
        
        async with self.order_lock:
            # ========== FISCALIZA BID (COMPRAR YES) ==========
            await self._fiscalize_bid()
            
            # ========== FISCALIZA ASK (VENDER YES via comprar NO) ==========
            await self._fiscalize_ask()
    
    async def _fiscalize_bid(self):
        """Fiscaliza e ajusta o BID (comprar YES)"""
        pos = self.position
        desired_bid = self.calculate_bid_price()
        
        # Não quota se posição máxima atingida
        should_quote_bid = pos < MAX_POSITION and desired_bid is not None
        
        if self.bid_order.state == OrderState.ACTIVE:
            # Tem ordem ativa - verifica se precisa cancelar
            if not should_quote_bid or self.should_cancel_bid(self.bid_order.price):
                # Precisa cancelar
                self.bid_order.state = OrderState.CANCELLING
                print(f"🔄 Cancelando BID @ {self.bid_order.price:.2f} (pred={self.price_pred:.4f}, pos={pos:.1f})")
                
                success = await asyncio.to_thread(
                    self._cancel_order_sync, 
                    self.bid_order.order_id
                )
                
                if success:
                    self.bid_order = OrderInfo()
                    print("✅ BID cancelado")
                else:
                    # Falhou ao cancelar - pode ter sido executado
                    self.bid_order = OrderInfo()
                    print("⚠️ BID: falha ao cancelar (pode ter sido executado)")
        
        elif self.bid_order.state == OrderState.CANCELLING:
            # Aguardando cancelamento - não faz nada
            pass
        
        elif self.bid_order.state == OrderState.NONE:
            # Sem ordem - verifica se deve colocar
            if should_quote_bid:
                order_id = await asyncio.to_thread(
                    self._place_order_sync,
                    self.token_id_yes,
                    desired_bid,
                    TRADE_SIZE,
                    BUY
                )
                
                if order_id:
                    self.bid_order = OrderInfo(
                        order_id=order_id,
                        price=desired_bid,
                        size=TRADE_SIZE,
                        state=OrderState.ACTIVE
                    )
                    print(f"📝 BID YES @ {desired_bid:.2f} | pos={pos:.1f} | pred={self.price_pred:.4f}")
    
    async def _fiscalize_ask(self):
        """Fiscaliza e ajusta o ASK (vender YES via comprar NO)"""
        pos = self.position
        desired_ask = self.calculate_ask_price()
        
        # Não quota se posição mínima atingida
        should_quote_ask = pos > -MAX_POSITION and desired_ask is not None
        
        # Preço do NO = 1 - ask_price
        no_price = round_price_down(1 - desired_ask) if desired_ask else None
        
        if self.ask_order.state == OrderState.ACTIVE:
            # Tem ordem ativa - verifica se precisa cancelar
            # ask_order.price guarda o preço do ASK (YES), não do NO
            if not should_quote_ask or self.should_cancel_ask(self.ask_order.price):
                # Precisa cancelar
                self.ask_order.state = OrderState.CANCELLING
                print(f"🔄 Cancelando ASK @ {self.ask_order.price:.2f} (pred={self.price_pred:.4f}, pos={pos:.1f})")
                
                success = await asyncio.to_thread(
                    self._cancel_order_sync, 
                    self.ask_order.order_id
                )
                
                if success:
                    self.ask_order = OrderInfo()
                    print("✅ ASK cancelado")
                else:
                    # Falhou ao cancelar - pode ter sido executado
                    self.ask_order = OrderInfo()
                    print("⚠️ ASK: falha ao cancelar (pode ter sido executado)")
        
        elif self.ask_order.state == OrderState.CANCELLING:
            # Aguardando cancelamento - não faz nada
            pass
        
        elif self.ask_order.state == OrderState.NONE:
            # Sem ordem - verifica se deve colocar
            if should_quote_ask and no_price is not None and no_price > 0:
                order_id = await asyncio.to_thread(
                    self._place_order_sync,
                    self.token_id_no,
                    no_price,
                    TRADE_SIZE,
                    BUY
                )
                
                if order_id:
                    self.ask_order = OrderInfo(
                        order_id=order_id,
                        price=desired_ask,  # Guarda o preço do ASK (YES)
                        size=TRADE_SIZE,
                        state=OrderState.ACTIVE
                    )
                    print(f"📝 ASK YES @ {desired_ask:.2f} (NO @ {no_price:.2f}) | pos={pos:.1f} | pred={self.price_pred:.4f}")
    
    async def _cancel_all_quotes(self):
        """Cancela todas as quotes (usado quando sai do horário de trading)"""
        if self.bid_order.state == OrderState.ACTIVE:
            self.bid_order.state = OrderState.CANCELLING
            await asyncio.to_thread(self._cancel_order_sync, self.bid_order.order_id)
            self.bid_order = OrderInfo()
        
        if self.ask_order.state == OrderState.ACTIVE:
            self.ask_order.state = OrderState.CANCELLING
            await asyncio.to_thread(self._cancel_order_sync, self.ask_order.order_id)
            self.ask_order = OrderInfo()
        
        print("🗑️ Quotes canceladas (fora do horário)")
    
    # ========== EVENT HANDLERS (APENAS ATUALIZAM ESTADO) ==========
    
    def on_bid_ask(self, data: dict):
        """Atualiza bid/ask do mercado (apenas estado, não toma ação)"""
        new_bid = data.get("best_bid")
        new_ask = data.get("best_ask")
        
        if new_bid is not None:
            self.best_bid = float(new_bid)
        if new_ask is not None:
            self.best_ask = float(new_ask)
    
    def on_price_pred(self, data: dict):
        """Atualiza preço previsto (apenas estado, não toma ação)"""
        if "pred_model2" in data:
            pred = data["pred_model2"]
            if pred is not None:
                self.price_pred = float(pred)
    
    def on_price_15(self, new_price_15: float):
        """Processa atualização do price_15 (detecta mudança de período)"""
        if self.price_15 is None:
            self.price_15 = new_price_15
            print(f"📊 Primeiro período detectado (price_15={new_price_15}). Aguardando próximo período.")
            return
        
        if new_price_15 != self.price_15:
            old_price = self.price_15
            self.price_15 = new_price_15
            
            # Calcula PnL do período
            if new_price_15 > old_price:
                period_pnl = self.pos_yes
                print(f"📈 Preço subiu! YES ganha. PnL período: +{period_pnl:.2f}")
            else:
                period_pnl = self.pos_no
                print(f"📉 Preço caiu! NO ganha. PnL período: +{period_pnl:.2f}")
            
            self.pnl += period_pnl
            print(f"💰 PnL total: {self.pnl:.2f}")
            
            # Reset e atualiza token IDs
            self.reset_period()
            print("\n🔄 Novo período! Renovando token IDs...")
            self.update_token_ids()
    
    async def on_fill(self, fill_data: dict):
        """Processa fill recebido do User Channel (com lock para evitar race condition)"""
        side = fill_data.get('side')
        outcome = fill_data.get('outcome')
        size = float(fill_data.get('size', 0))
        price = float(fill_data.get('price', 0))
        is_maker = fill_data.get('is_maker', False)
        
        if size <= 0:
            return
        
        outcome_upper = outcome.upper() if outcome else ''
        
        # Adquire lock para evitar race condition com fiscalize_quotes
        async with self.order_lock:
            # Atualiza posição
            if side == 'BUY':
                if outcome_upper == 'UP':
                    self.pos_yes += size
                    # Se era nosso BID, limpa o estado
                    if self.bid_order.state == OrderState.ACTIVE:
                        self.bid_order = OrderInfo()
                elif outcome_upper == 'DOWN':
                    self.pos_no += size
                    # Se era nosso ASK (compra NO), limpa o estado
                    if self.ask_order.state == OrderState.ACTIVE:
                        self.ask_order = OrderInfo()
            elif side == 'SELL':
                if outcome_upper == 'UP':
                    self.pos_yes -= size
                elif outcome_upper == 'DOWN':
                    self.pos_no -= size
            
            self.total_traded += size
        
        maker_str = "MAKER" if is_maker else "TAKER"
        print(f"💰 FILL [{maker_str}]: {side} {size} {outcome} @ {price:.4f}")
        print(f"📊 Posição: YES={self.pos_yes:.2f} | NO={self.pos_no:.2f}")


# ============================================================
# IPC READERS (APENAS ATUALIZAM ESTADO)
# ============================================================

async def read_price_pred(zmq_context, maker: MarketMakerV2):
    """Lê price_pred (pred_model2) do process-bitcoin-price.py"""
    socket = zmq_context.socket(zmq.PULL)
    socket.connect(IPC_PRICE_PRED)
    print(f"🔌 Conectado ao IPC price_pred: {IPC_PRICE_PRED}")
    
    first_msg = True
    while True:
        try:
            data = await socket.recv_json()
            
            if first_msg:
                pred = data.get('pred_model2')
                pred_str = f"{pred:.4f}" if pred is not None else "N/A"
                print(f"✅ Primeiro price_pred: {pred_str}")
                first_msg = False
            
            maker.on_price_pred(data)
        
        except zmq.ZMQError as e:
            if e.errno == zmq.ETERM:
                break
            print(f"❌ Erro ZMQ price_pred: {e}")
            await asyncio.sleep(0.1)
        except asyncio.CancelledError:
            break
        except Exception as e:
            print(f"❌ Erro price_pred: {e}")
            await asyncio.sleep(0.1)
    
    socket.close()


async def read_bid_ask(zmq_context, maker: MarketMakerV2):
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


async def read_price_15(zmq_context, maker: MarketMakerV2):
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


async def read_fills(zmq_context, maker: MarketMakerV2):
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
            
            await maker.on_fill(data)
        
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


async def status_printer(maker: MarketMakerV2):
    """Imprime status periodicamente"""
    while True:
        await asyncio.sleep(5)
        
        if maker.price_pred is not None:
            tm_seconds = time.time() % 900
            t = tm_seconds / 60
            pos = maker.position
            
            can_trade = "✅" if maker._can_trade() else "❌"
            
            # Status das ordens
            bid_status = f"BID@{maker.bid_order.price:.2f}" if maker.bid_order.state == OrderState.ACTIVE else "---"
            ask_status = f"ASK@{maker.ask_order.price:.2f}" if maker.ask_order.state == OrderState.ACTIVE else "---"
            
            # Preços desejados
            desired_bid = maker.calculate_bid_price()
            desired_ask = maker.calculate_ask_price()
            desired_bid_str = f"{desired_bid:.2f}" if desired_bid else "N/A"
            desired_ask_str = f"{desired_ask:.2f}" if desired_ask else "N/A"
            
            # Bid/ask do mercado
            mkt_bid_str = f"{maker.best_bid:.2f}" if maker.best_bid else "N/A"
            mkt_ask_str = f"{maker.best_ask:.2f}" if maker.best_ask else "N/A"
            
            print(
                f"📊 pred={maker.price_pred:.4f} | "
                f"mkt: {mkt_bid_str}/{mkt_ask_str} | "
                f"quotes: {bid_status}/{ask_status} | "
                f"desired: {desired_bid_str}/{desired_ask_str} | "
                f"pos: {maker.pos_yes:.1f}Y/{maker.pos_no:.1f}N ({pos:+.1f}) | "
                f"pnl={maker.pnl:.2f} | "
                f"t={t:.1f}min {can_trade}"
            )


# ============================================================
# MAIN
# ============================================================

async def main():
    """Loop principal do Market Maker V2"""
    print("=" * 70)
    print("POLYMARKET MARKET MAKER V2")
    print("=" * 70)
    print(f"📋 Estratégia:")
    print(f"   - BID  = min(floor(price_pred - {SPREAD_QUOTE} - {SKEW_FACTOR}*pos), best_bid)")
    print(f"   - ASK  = max(ceil(price_pred + {SPREAD_QUOTE} - {SKEW_FACTOR}*pos), best_ask)")
    print(f"   - Cancel BID se price > price_pred - {SPREAD_CANCEL} - {SKEW_FACTOR}*pos")
    print(f"   - Cancel BID se price < best_bid - {MAX_DISTANCE}")
    print(f"   - Cancel ASK se price < price_pred + {SPREAD_CANCEL} - {SKEW_FACTOR}*pos")
    print(f"   - Cancel ASK se price > best_ask + {MAX_DISTANCE}")
    print(f"   - Fiscaliza a cada: {FISCALIZE_INTERVAL}s")
    print(f"   - Trade size: {TRADE_SIZE}, Max position: ±{MAX_POSITION}")
    print(f"   - Horário: {NO_TRADE_START:.1f} - {NO_TRADE_END:.1f} min")
    print("=" * 70)
    
    print("\n🔐 Autenticando no Polymarket...")
    try:
        client = authenticate_polymarket()
        print("✅ Autenticação bem sucedida!")
    except Exception as e:
        print(f"❌ Erro na autenticação: {e}")
        return
    
    maker = MarketMakerV2(client)
    
    print("\n🔄 Buscando token IDs do mercado atual...")
    if not maker.update_token_ids():
        print("⚠️ Falha ao buscar token IDs iniciais (pode funcionar depois)")
    
    zmq_context = zmq.asyncio.Context()
    
    print("\n🚀 Iniciando Market Maker V2...")
    print(f"📡 IPC price_pred: {IPC_PRICE_PRED}")
    print(f"📡 IPC mercado: {IPC_MARKET}")
    print(f"📡 IPC price_15: {IPC_PRICE15}")
    print(f"📡 IPC fills: {IPC_FILLS}")
    print("\n⏳ Aguardando dados...\n")
    
    tasks = [
        asyncio.create_task(read_price_pred(zmq_context, maker)),
        asyncio.create_task(read_bid_ask(zmq_context, maker)),
        asyncio.create_task(read_price_15(zmq_context, maker)),
        asyncio.create_task(read_fills(zmq_context, maker)),
        asyncio.create_task(maker.fiscalize_loop()),  # Loop de fiscalização
        asyncio.create_task(status_printer(maker)),
    ]
    
    try:
        await asyncio.gather(*tasks)
    except KeyboardInterrupt:
        print("\n👋 Encerrando...")
    finally:
        print("🗑️ Cancelando ordens pendentes...")
        maker._cancel_all_orders_sync()
        
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        zmq_context.term()
        print("✅ Encerrado!")


if __name__ == "__main__":
    asyncio.run(main())
