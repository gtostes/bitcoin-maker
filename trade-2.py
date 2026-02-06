#!/usr/bin/env python3
"""
POLYMARKET MARKET MAKER V2

Estratégia:
- Fiscaliza quotes a cada 0.2s (não reage a eventos)
- Compra pelo maior preço <= min(price_pred - SPREAD_QUOTE - SKEW*pos, best_bid)
- Vende pelo menor preço >= max(price_pred + SPREAD_QUOTE - SKEW*pos, best_ask)
- Cancela BID se:
  - |my_bid - desired_bid| > SPREAD_CANCEL (longe da quote ideal)
  - my_bid < best_bid - MAX_DISTANCE (muito longe do mercado)
- Cancela ASK se:
  - |my_ask - desired_ask| > SPREAD_CANCEL (longe da quote ideal)
  - my_ask > best_ask + MAX_DISTANCE (muito longe do mercado)
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
from py_clob_client.clob_types import OrderArgs, OrderType, OpenOrderParams
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
SPREAD_QUOTE = 0.04       # Spread para colocar quote (4 cents cada lado)
SPREAD_CANCEL = 0.018     # Distância máxima da quote ideal para cancelar
SKEW_FACTOR = 0.0024       # Fator de skew por unidade de posição
MAX_DISTANCE = 0.10       # Distância máxima do mercado para cancelar (10 cents)

# Intervalos de tempo
FISCALIZE_INTERVAL = 0.2  # Fiscaliza quotes a cada 200ms
NO_TRADE_START = 0.5      # Não opera nos primeiros 30s (0.5 min)
NO_TRADE_END = 14.5       # Não opera depois de 14:30 (14.5 min)
WARMUP_SECONDS = 30       # Tempo mínimo de warmup após iniciar (30s)

# Tick size do Polymarket
TICK_SIZE = Decimal("0.01")


# ============================================================
# ASYNC PRINT (não bloqueia o event loop)
# ============================================================

import datetime
from pathlib import Path

_print_queue: asyncio.Queue = None
_log_file = None
_current_id_time = None

def _get_id_time() -> int:
    """Retorna o id_time do período atual de 15min"""
    return 900 * int(time.time() // 900)

def _ensure_log_file():
    """Garante que o arquivo de log está aberto para o período atual"""
    global _log_file, _current_id_time
    
    id_time = _get_id_time()
    
    # Se mudou de período, fecha o arquivo antigo e abre novo
    if id_time != _current_id_time:
        if _log_file is not None:
            try:
                _log_file.close()
            except:
                pass
        
        # Cria diretório se não existir
        log_dir = Path(__file__).parent / "data"
        log_dir.mkdir(exist_ok=True)
        
        log_path = log_dir / f"logs-{id_time}.txt"
        _log_file = open(log_path, "a", buffering=1)  # Line buffered
        _current_id_time = id_time
        
        # Marca início de sessão
        _log_file.write(f"\n{'='*60}\n")
        _log_file.write(f"SESSÃO INICIADA: {datetime.datetime.now().isoformat()}\n")
        _log_file.write(f"{'='*60}\n\n")
    
    return _log_file

def aprint(msg: str):
    """
    Print assíncrono - enfileira a mensagem com timestamp para ser printada por uma task separada.
    Também escreve em arquivo de log.
    NUNCA bloqueia a função que chamou.
    """
    global _print_queue
    # Adiciona timestamp no momento do enfileiramento (não no momento do print)
    ts = datetime.datetime.now().strftime("%H:%M:%S.%f")[:-3]
    timestamped_msg = f"[{ts}] {msg}"
    
    if _print_queue is not None:
        try:
            _print_queue.put_nowait(timestamped_msg)
        except asyncio.QueueFull:
            pass  # Descarta se a fila estiver cheia (evita bloqueio)

async def _print_worker():
    """Worker que processa a fila de prints em background."""
    global _print_queue
    _print_queue = asyncio.Queue(maxsize=1000)
    
    while True:
        try:
            msg = await _print_queue.get()
            
            # Print para terminal
            print(msg)
            
            # Escreve no arquivo de log (síncrono, mas é só append)
            try:
                log_file = _ensure_log_file()
                if log_file:
                    log_file.write(msg + "\n")
            except Exception:
                pass  # Ignora erros de I/O no log
            
            _print_queue.task_done()
        except asyncio.CancelledError:
            # Drena a fila antes de sair
            while not _print_queue.empty():
                try:
                    msg = _print_queue.get_nowait()
                    print(msg)
                    try:
                        log_file = _ensure_log_file()
                        if log_file:
                            log_file.write(msg + "\n")
                    except:
                        pass
                except asyncio.QueueEmpty:
                    break
            # Fecha o arquivo de log
            if _log_file is not None:
                try:
                    _log_file.close()
                except:
                    pass
            break
        except Exception:
            pass


# ============================================================
# HELPERS§
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
            aprint(f"❌ Nenhum mercado encontrado para {url}")
            return None, None
        
        ids = re.findall(r'"(\d+)"', markets[0]['clobTokenIds'])
        
        if len(ids) >= 2:
            token_id_yes = ids[0]
            token_id_no = ids[1]
            aprint(f"✅ Token IDs: YES={token_id_yes[:20]}... | NO={token_id_no[:20]}...")
            return token_id_yes, token_id_no
        else:
            aprint(f"❌ Token IDs insuficientes: {ids}")
            return None, None
            
    except Exception as e:
        aprint(f"❌ Erro ao buscar token IDs: {e}")
        return None, None


# ============================================================
# ORDER STATE & TRACKING
# ============================================================

class OrderState(Enum):
    """Estado de uma ordem"""
    NONE = auto()           # Sem ordem
    ACTIVE = auto()         # Ordem ativa no mercado
    CANCELLING = auto()     # Cancelando (aguardando confirmação)


class OrderStatus(Enum):
    """Status do ciclo de vida de uma ordem"""
    CREATED = "created"         # Ordem criada
    CANCEL_SENT = "cancel_sent" # Cancel enviado
    CANCELLED = "cancelled"     # Confirmado cancelado
    FILLED = "filled"           # Executado
    UNKNOWN = "unknown"         # Estado desconhecido


@dataclass
class OrderHistory:
    """Histórico completo de uma ordem"""
    order_id: str
    side: str  # "BID" ou "ASK"
    price: float
    size: float
    created_at: float  # timestamp
    status: OrderStatus = OrderStatus.CREATED
    cancel_sent_at: float = None
    cancelled_at: float = None
    filled_at: float = None
    fill_price: float = None
    notes: str = ""


@dataclass
class OrderInfo:
    """Informações de uma ordem"""
    order_id: str = None
    price: float = None
    size: float = None              # Tamanho original da ordem
    filled_size: float = 0.0        # Quanto já foi executado (para partial fills)
    state: OrderState = OrderState.NONE
    
    @property
    def remaining_size(self) -> float:
        """Tamanho restante da ordem"""
        return self.size - self.filled_size if self.size else 0.0
    
    @property
    def is_fully_filled(self) -> bool:
        """Verifica se a ordem foi completamente executada"""
        if self.size is None:
            return False
        return self.filled_size >= self.size - 0.001  # Tolerância para float

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
        
        # Estado das ordens atuais (BID = comprar YES, ASK = vender YES via comprar NO)
        self.bid_order = OrderInfo()
        self.ask_order = OrderInfo()
        
        # Histórico completo de todas as ordens (order_id -> OrderHistory)
        self.order_history: dict[str, OrderHistory] = {}
        
        # Set de order_ids que estão pendentes de confirmação de cancel
        self.pending_cancel: set = set()
        
        # PnL tracking
        self.pnl = 0.0
        self.total_traded = 0.0
        
        # Estado de controle
        self.bot_start_time = time.time()  # Tempo que o bot iniciou
        
        # Lock para operações de ordem (evita race conditions)
        self.order_lock = asyncio.Lock()
        
        # Contadores para monitoramento
        self.stats = {
            "orders_created": 0,
            "orders_cancelled": 0,
            "orders_filled": 0,
            "cancel_failures": 0,
            "orphan_orders_found": 0,
        }
    
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
            aprint(f"📊 Período encerrado | Posição: YES={self.pos_yes:.2f} NO={self.pos_no:.2f}")
        
        # Cancela ordens pendentes
        self._cancel_all_orders_sync()
        
        # Reset estado
        self.pos_yes = 0.0
        self.pos_no = 0.0
        self.bid_order = OrderInfo()
        self.ask_order = OrderInfo()
        
        aprint("🔄 Reset para novo período")
    
    def _can_trade(self) -> bool:
        """Verifica se pode operar no momento atual"""
        # Espera warmup mínimo de 30s após iniciar o bot
        if time.time() - self.bot_start_time < WARMUP_SECONDS:
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
        1. |current_price - desired_bid| > SPREAD_CANCEL (longe da quote ideal)
        2. current_price < best_bid - MAX_DISTANCE (muito longe do mercado)
        """
        # Condição 1: longe da quote ideal
        desired_bid = self.calculate_bid_price()
        if desired_bid is not None:
            if abs(current_price - desired_bid) > SPREAD_CANCEL:
                return True
        else:
            # Sem preço desejado válido, cancela
            return True
        
        # Condição 2: preço muito longe do mercado
        if self.best_bid is not None and current_price < self.best_bid - MAX_DISTANCE:
            return True
        
        return False
    
    def should_cancel_ask(self, current_price: float) -> bool:
        """
        Verifica se deve cancelar ASK.
        Cancela se:
        1. |current_price - desired_ask| > SPREAD_CANCEL (longe da quote ideal)
        2. current_price > best_ask + MAX_DISTANCE (muito longe do mercado)
        """
        # Condição 1: longe da quote ideal
        desired_ask = self.calculate_ask_price()
        if desired_ask is not None:
            if abs(current_price - desired_ask) > SPREAD_CANCEL:
                return True
        else:
            # Sem preço desejado válido, cancela
            return True
        
        # Condição 2: preço muito longe do mercado
        if self.best_ask is not None and current_price > self.best_ask + MAX_DISTANCE:
            return True
        
        return False
    
    # ========== OPERAÇÕES DE ORDEM (SÍNCRONAS) ==========
    
    def _place_order_sync(self, token_id: str, price: float, size: float, side: str, order_side: str) -> str | None:
        """
        Coloca uma ordem GTC (maker) de forma síncrona.
        order_side: "BID" ou "ASK" para tracking
        """
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
                order_id = resp['orderID']
                
                # Registra no histórico
                self.order_history[order_id] = OrderHistory(
                    order_id=order_id,
                    side=order_side,
                    price=price,
                    size=size,
                    created_at=time.time(),
                    status=OrderStatus.CREATED
                )
                self.stats["orders_created"] += 1
                
                return order_id
            return None
            
        except Exception as e:
            aprint(f"⚠️ Place order exception: {e}")
            return None
    
    def _cancel_order_sync(self, order_id: str) -> tuple[bool, str]:
        """
        Cancela uma ordem de forma síncrona.
        Retorna (sucesso, motivo)
        """
        try:
            # Marca no histórico
            if order_id in self.order_history:
                self.order_history[order_id].status = OrderStatus.CANCEL_SENT
                self.order_history[order_id].cancel_sent_at = time.time()
            
            self.pending_cancel.add(order_id)
            
            resp = self.client.cancel(order_id=order_id)
            
            # Analisa resposta
            canceled = resp.get('canceled', []) if resp else []
            not_canceled = resp.get('not_canceled', {}) if resp else {}
            
            if order_id in canceled:
                # Confirmado cancelado
                if order_id in self.order_history:
                    self.order_history[order_id].status = OrderStatus.CANCELLED
                    self.order_history[order_id].cancelled_at = time.time()
                self.pending_cancel.discard(order_id)
                self.stats["orders_cancelled"] += 1
                aprint(f"✅ Cancel CONFIRMADO: {order_id[:16]}...")
                return True, "confirmed"
            
            elif order_id in not_canceled:
                reason = not_canceled[order_id]
                if order_id in self.order_history:
                    self.order_history[order_id].notes = f"cancel failed: {reason}"
                self.pending_cancel.discard(order_id)
                self.stats["cancel_failures"] += 1
                aprint(f"❌ Cancel FALHOU: {order_id[:16]}... reason={reason}")
                return False, reason
            
            else:
                # Resposta não contém o order_id - pode ter sido executado
                self.pending_cancel.discard(order_id)
                aprint(f"⚠️ Cancel response sem order_id: {resp}")
                return False, "not_in_response"
            
        except Exception as e:
            self.pending_cancel.discard(order_id)
            aprint(f"⚠️ Cancel exception: {order_id[:16]}... {e}")
            return False, str(e)
    
    def _cancel_all_orders_sync(self):
        """Cancela todas as ordens ativas conhecidas"""
        order_ids = []
        if self.bid_order.state == OrderState.ACTIVE and self.bid_order.order_id:
            order_ids.append(self.bid_order.order_id)
        if self.ask_order.state == OrderState.ACTIVE and self.ask_order.order_id:
            order_ids.append(self.ask_order.order_id)
        
        if order_ids:
            try:
                resp = self.client.cancel_orders(order_ids)
                canceled = resp.get('canceled', []) if resp else []
                not_canceled = resp.get('not_canceled', {}) if resp else {}
                aprint(f"🗑️ Cancel batch: canceled={len(canceled)}, failed={len(not_canceled)}")
            except Exception as e:
                aprint(f"❌ Erro ao cancelar ordens: {e}")
    
    def cancel_all_open_orders(self):
        """Cancela TODAS as ordens abertas via API (não só as que conhecemos)"""
        try:
            # Usa cancel_all da API para garantir que não tem ordens penduradas
            resp = self.client.cancel_all()
            aprint(f"🧹 Cancel ALL response: {resp}")
            return True
        except Exception as e:
            aprint(f"⚠️ Cancel ALL falhou: {e}")
            return False
    
    # ========== LOOP PRINCIPAL DE FISCALIZAÇÃO ==========
    
    async def fiscalize_loop(self):
        """
        Loop principal que fiscaliza as quotes a cada 0.2s.
        Este é o ÚNICO lugar que decide colocar/cancelar ordens.
        """
        aprint("🔍 Iniciando loop de fiscalização...")
        
        while True:
            try:
                await asyncio.sleep(FISCALIZE_INTERVAL)
                await self._fiscalize_quotes()
            except asyncio.CancelledError:
                break
            except Exception as e:
                aprint(f"❌ Erro no loop de fiscalização: {e}")
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
        
        # Não quota se posição máxima atingida ou preço inválido
        should_quote_bid = pos < MAX_POSITION and desired_bid is not None
        
        # Também não quota se estaria muito longe do mercado
        if should_quote_bid and self.best_bid is not None:
            if desired_bid < self.best_bid - MAX_DISTANCE:
                should_quote_bid = False
        
        if self.bid_order.state == OrderState.ACTIVE:
            # Tem ordem ativa - verifica se precisa cancelar
            needs_cancel = not should_quote_bid or self.should_cancel_bid(self.bid_order.price)
            
            # Log se tem partial fill
            if self.bid_order.filled_size > 0:
                aprint(f"📊 BID tem partial fill: {self.bid_order.filled_size:.1f}/{self.bid_order.size:.1f}")
            
            if needs_cancel:
                # Precisa cancelar - guarda info para print DEPOIS
                old_price = self.bid_order.price
                old_pred = self.price_pred
                old_filled = self.bid_order.filled_size
                
                self.bid_order.state = OrderState.CANCELLING
                success, reason = await asyncio.to_thread(
                    self._cancel_order_sync, 
                    self.bid_order.order_id
                )
                self.bid_order = OrderInfo()
                
                # Print DEPOIS da ação
                if success:
                    aprint(f"✅ BID cancelado @ {old_price:.2f} (pred={old_pred:.4f}, pos={pos:.1f}, filled={old_filled:.1f})")
                else:
                    aprint(f"⚠️ BID cancel failed @ {old_price:.2f} reason={reason} (filled={old_filled:.1f})")
        
        elif self.bid_order.state == OrderState.CANCELLING:
            # Aguardando cancelamento - não faz nada
            pass
        
        elif self.bid_order.state == OrderState.NONE:
            # Sem ordem - verifica se deve colocar
            if should_quote_bid:
                # Guarda pred ANTES do await (pode mudar durante a chamada)
                pred_at_order = self.price_pred
                
                order_id = await asyncio.to_thread(
                    self._place_order_sync,
                    self.token_id_yes,
                    desired_bid,
                    TRADE_SIZE,
                    BUY,
                    "BID"  # order_side para tracking
                )
                
                if order_id:
                    self.bid_order = OrderInfo(
                        order_id=order_id,
                        price=desired_bid,
                        size=TRADE_SIZE,
                        filled_size=0.0,
                        state=OrderState.ACTIVE
                    )
                    # Print DEPOIS da ação (com pred do momento da decisão)
                    aprint(f"📝 BID YES @ {desired_bid:.2f} | pos={pos:.1f} | pred={pred_at_order:.4f} | id={order_id[:16]}...")
    
    async def _fiscalize_ask(self):
        """Fiscaliza e ajusta o ASK (vender YES via comprar NO)"""
        pos = self.position
        desired_ask = self.calculate_ask_price()
        
        # Não quota se posição mínima atingida ou preço inválido
        should_quote_ask = pos > -MAX_POSITION and desired_ask is not None
        
        # Também não quota se estaria muito longe do mercado
        if should_quote_ask and self.best_ask is not None:
            if desired_ask > self.best_ask + MAX_DISTANCE:
                should_quote_ask = False
        
        # Preço do NO = 1 - ask_price
        no_price = round_price_down(1 - desired_ask) if desired_ask else None
        
        if self.ask_order.state == OrderState.ACTIVE:
            # Tem ordem ativa - verifica se precisa cancelar
            # ask_order.price guarda o preço do ASK (YES), não do NO
            needs_cancel = not should_quote_ask or self.should_cancel_ask(self.ask_order.price)
            
            # Log se tem partial fill
            if self.ask_order.filled_size > 0:
                aprint(f"📊 ASK tem partial fill: {self.ask_order.filled_size:.1f}/{self.ask_order.size:.1f}")
            
            if needs_cancel:
                # Precisa cancelar - guarda info para print DEPOIS
                old_price = self.ask_order.price
                old_pred = self.price_pred
                old_filled = self.ask_order.filled_size
                
                self.ask_order.state = OrderState.CANCELLING
                success, reason = await asyncio.to_thread(
                    self._cancel_order_sync, 
                    self.ask_order.order_id
                )
                self.ask_order = OrderInfo()
                
                # Print DEPOIS da ação
                if success:
                    aprint(f"✅ ASK cancelado @ {old_price:.2f} (pred={old_pred:.4f}, pos={pos:.1f}, filled={old_filled:.1f})")
                else:
                    aprint(f"⚠️ ASK cancel failed @ {old_price:.2f} reason={reason} (filled={old_filled:.1f})")
        
        elif self.ask_order.state == OrderState.CANCELLING:
            # Aguardando cancelamento - não faz nada
            pass
        
        elif self.ask_order.state == OrderState.NONE:
            # Sem ordem - verifica se deve colocar
            if should_quote_ask and no_price is not None and no_price > 0:
                # Guarda pred ANTES do await (pode mudar durante a chamada)
                pred_at_order = self.price_pred
                
                order_id = await asyncio.to_thread(
                    self._place_order_sync,
                    self.token_id_no,
                    no_price,
                    TRADE_SIZE,
                    BUY,
                    "ASK"  # order_side para tracking
                )
                
                if order_id:
                    self.ask_order = OrderInfo(
                        order_id=order_id,
                        price=desired_ask,  # Guarda o preço do ASK (YES)
                        size=TRADE_SIZE,
                        filled_size=0.0,
                        state=OrderState.ACTIVE
                    )
                    # Print DEPOIS da ação (com pred do momento da decisão)
                    aprint(f"📝 ASK YES @ {desired_ask:.2f} (NO @ {no_price:.2f}) | pos={pos:.1f} | pred={pred_at_order:.4f} | id={order_id[:16]}...")
    
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
        
        # Print DEPOIS da ação
        aprint("🗑️ Quotes canceladas (fora do horário)")
    
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
            # Primeiro price_15 recebido - apenas salva
            self.price_15 = new_price_15
            aprint(f"📊 Primeiro price_15 recebido: {new_price_15}")
            return
        
        if new_price_15 != self.price_15:
            old_price = self.price_15
            self.price_15 = new_price_15
            
            # Calcula PnL do período
            if new_price_15 > old_price:
                period_pnl = self.pos_yes
                aprint(f"📈 Preço subiu! YES ganha. PnL período: +{period_pnl:.2f}")
            else:
                period_pnl = self.pos_no
                aprint(f"📉 Preço caiu! NO ganha. PnL período: +{period_pnl:.2f}")
            
            self.pnl += period_pnl
            aprint(f"💰 PnL total: {self.pnl:.2f}")
            
            # Reset e atualiza token IDs
            self.reset_period()
            aprint("\n🔄 Novo período! Renovando token IDs...")
            self.update_token_ids()
    
    async def on_fill(self, fill_data: dict):
        """
        Processa fill recebido do User Channel.
        
        LÓGICA DE PARTIAL FILLS:
        - Se a ordem foi parcialmente executada, apenas atualiza filled_size
        - A ordem permanece ACTIVE até ser totalmente executada
        - Só marca como NONE quando is_fully_filled ou quando não conseguimos identificar
        """
        side = fill_data.get('side')
        outcome = fill_data.get('outcome')
        size = float(fill_data.get('size', 0))
        price = float(fill_data.get('price', 0))
        is_maker = fill_data.get('is_maker', False)
        order_id = fill_data.get('order_id')
        
        if size <= 0:
            return
        
        outcome_upper = outcome.upper() if outcome else ''
        
        # Identifica qual ordem foi executada
        is_bid_fill = False
        is_ask_fill = False
        
        # BUY UP = comprou YES = BID order
        # BUY DOWN = comprou NO = ASK order (vendemos YES comprando NO)
        if side == 'BUY':
            if outcome_upper == 'UP':
                is_bid_fill = True
            elif outcome_upper == 'DOWN':
                is_ask_fill = True
        
        # Verifica se o order_id bate com nossa ordem atual
        order_matches_bid = order_id and self.bid_order.order_id == order_id
        order_matches_ask = order_id and self.ask_order.order_id == order_id
        
        # Log detalhado
        order_id_short = order_id[:16] if order_id else "N/A"
        aprint(f"📥 FILL recebido: {side} {size} {outcome} @ {price:.4f} order={order_id_short}...")
        
        # Atualiza histórico da ordem
        if order_id and order_id in self.order_history:
            hist = self.order_history[order_id]
            # Não marca como FILLED ainda se for partial
            hist.filled_at = time.time()
            hist.fill_price = price
        elif order_id:
            aprint(f"⚠️ FILL de ordem DESCONHECIDA: order={order_id_short}...")
        
        # Remove de pending_cancel se estava lá
        if order_id:
            self.pending_cancel.discard(order_id)
        
        async with self.order_lock:
            # ========== PROCESSA BID FILL ==========
            if is_bid_fill:
                if order_matches_bid or (not order_id and self.bid_order.state == OrderState.ACTIVE):
                    # É a nossa ordem de BID
                    old_filled = self.bid_order.filled_size
                    self.bid_order.filled_size += size
                    
                    aprint(f"📊 BID fill: {old_filled:.1f} -> {self.bid_order.filled_size:.1f} / {self.bid_order.size:.1f}")
                    
                    if self.bid_order.is_fully_filled:
                        # Ordem completamente executada
                        aprint(f"✅ BID FULLY FILLED @ {self.bid_order.price:.2f}")
                        if order_id and order_id in self.order_history:
                            self.order_history[order_id].status = OrderStatus.FILLED
                        self.stats["orders_filled"] += 1
                        self.bid_order = OrderInfo()  # Limpa a ordem
                    else:
                        # Partial fill - ordem continua ativa
                        aprint(f"⏳ BID PARTIAL FILL: remaining={self.bid_order.remaining_size:.1f}")
                        # NÃO limpa a ordem, ela continua ACTIVE
                else:
                    # Fill de ordem que não é a atual (órfã ou antiga)
                    aprint(f"⚠️ BID fill de ordem diferente da atual (atual={self.bid_order.order_id[:16] if self.bid_order.order_id else 'None'}...)")
                
                # Atualiza posição independente
                self.pos_yes += size
                self.pnl -= size * price
            
            # ========== PROCESSA ASK FILL ==========
            elif is_ask_fill:
                if order_matches_ask or (not order_id and self.ask_order.state == OrderState.ACTIVE):
                    # É a nossa ordem de ASK
                    old_filled = self.ask_order.filled_size
                    self.ask_order.filled_size += size
                    
                    aprint(f"📊 ASK fill: {old_filled:.1f} -> {self.ask_order.filled_size:.1f} / {self.ask_order.size:.1f}")
                    
                    if self.ask_order.is_fully_filled:
                        # Ordem completamente executada
                        aprint(f"✅ ASK FULLY FILLED @ {self.ask_order.price:.2f}")
                        if order_id and order_id in self.order_history:
                            self.order_history[order_id].status = OrderStatus.FILLED
                        self.stats["orders_filled"] += 1
                        self.ask_order = OrderInfo()  # Limpa a ordem
                    else:
                        # Partial fill - ordem continua ativa
                        aprint(f"⏳ ASK PARTIAL FILL: remaining={self.ask_order.remaining_size:.1f}")
                        # NÃO limpa a ordem, ela continua ACTIVE
                else:
                    # Fill de ordem que não é a atual (órfã ou antiga)
                    aprint(f"⚠️ ASK fill de ordem diferente da atual (atual={self.ask_order.order_id[:16] if self.ask_order.order_id else 'None'}...)")
                
                # Atualiza posição independente
                self.pos_no += size
                self.pnl -= size * price
            
            # ========== SELL (raro, mas tratamos) ==========
            elif side == 'SELL':
                if outcome_upper == 'UP':
                    self.pos_yes -= size
                    self.pnl += size * price
                elif outcome_upper == 'DOWN':
                    self.pos_no -= size
                    self.pnl += size * price
            
            self.total_traded += size
        
        maker_str = "MAKER" if is_maker else "TAKER"
        aprint(f"💰 FILL [{maker_str}]: {side} {size} {outcome} @ {price:.4f}")
        aprint(f"📊 Posição: YES={self.pos_yes:.2f} | NO={self.pos_no:.2f}")


# ============================================================
# IPC READERS (APENAS ATUALIZAM ESTADO)
# ============================================================

async def read_price_pred(zmq_context, maker: MarketMakerV2):
    """Lê price_pred (pred_model2) do process-bitcoin-price.py"""
    socket = zmq_context.socket(zmq.PULL)
    socket.connect(IPC_PRICE_PRED)
    aprint(f"🔌 Conectado ao IPC price_pred: {IPC_PRICE_PRED}")
    
    first_msg = True
    while True:
        try:
            data = await socket.recv_json()
            
            if first_msg:
                pred = data.get('pred_model2')
                pred_str = f"{pred:.4f}" if pred is not None else "N/A"
                aprint(f"✅ Primeiro price_pred: {pred_str}")
                first_msg = False
            
            maker.on_price_pred(data)
        
        except zmq.ZMQError as e:
            if e.errno == zmq.ETERM:
                break
            aprint(f"❌ Erro ZMQ price_pred: {e}")
            await asyncio.sleep(0.1)
        except asyncio.CancelledError:
            break
        except Exception as e:
            aprint(f"❌ Erro price_pred: {e}")
            await asyncio.sleep(0.1)
    
    socket.close()


async def read_bid_ask(zmq_context, maker: MarketMakerV2):
    """Lê bid/ask do market-user-stream.py"""
    socket = zmq_context.socket(zmq.PULL)
    socket.connect(IPC_MARKET)
    aprint(f"🔌 Conectado ao IPC mercado: {IPC_MARKET}")
    
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
                aprint(f"✅ Primeiro bid/ask: bid={data['best_bid']} | ask={data['best_ask']}")
                first_msg = False
            
            maker.on_bid_ask(data)
        
        except zmq.ZMQError as e:
            if e.errno == zmq.ETERM:
                break
            aprint(f"❌ Erro ZMQ bid/ask: {e}")
            await asyncio.sleep(0.1)
        except asyncio.CancelledError:
            break
        except Exception as e:
            aprint(f"❌ Erro bid/ask: {e}")
            await asyncio.sleep(0.1)
    
    socket.close()


async def read_price_15(zmq_context, maker: MarketMakerV2):
    """Lê price_15 (detecta mudança de período)"""
    socket = zmq_context.socket(zmq.SUB)
    socket.connect(IPC_PRICE15)
    socket.setsockopt_string(zmq.SUBSCRIBE, "")
    aprint(f"🔌 Conectado ao IPC price_15: {IPC_PRICE15}")
    
    first_msg = True
    while True:
        try:
            data = await socket.recv_json()
            
            if "price_15" in data:
                if first_msg:
                    aprint(f"✅ Primeiro price_15: {data['price_15']}")
                    first_msg = False
                maker.on_price_15(data["price_15"])
        
        except zmq.ZMQError as e:
            if e.errno == zmq.ETERM:
                break
            aprint(f"❌ Erro ZMQ price_15: {e}")
            await asyncio.sleep(0.1)
        except asyncio.CancelledError:
            break
        except Exception as e:
            aprint(f"❌ Erro price_15: {e}")
            await asyncio.sleep(0.1)
    
    socket.close()


async def read_fills(zmq_context, maker: MarketMakerV2):
    """Lê fills do market-user-stream.py"""
    socket = zmq_context.socket(zmq.PULL)
    socket.connect(IPC_FILLS)
    aprint(f"🔌 Conectado ao IPC fills: {IPC_FILLS}")
    
    first_msg = True
    while True:
        try:
            data = await socket.recv_json()
            
            # IMPORTANTE: Processa o fill ANTES de qualquer outra coisa
            # para evitar race condition com fiscalize_loop
            await maker.on_fill(data)
            
            if first_msg:
                aprint(f"✅ Primeiro fill processado")
                first_msg = False
        
        except zmq.ZMQError as e:
            if e.errno == zmq.ETERM:
                break
            aprint(f"❌ Erro ZMQ fills: {e}")
            await asyncio.sleep(0.1)
        except asyncio.CancelledError:
            break
        except Exception as e:
            aprint(f"❌ Erro fills: {e}")
            await asyncio.sleep(0.1)
    
    socket.close()


# ============================================================
# AUDITOR DE ORDENS (VERIFICA ORDENS ÓRFÃS)
# ============================================================

async def order_auditor(maker: MarketMakerV2):
    """
    Audita ordens a cada 1s para detectar ordens órfãs.
    Busca ordens ativas na API e compara com o estado local.
    """
    aprint("🔍 Iniciando auditor de ordens...")
    
    # Espera 5s para o bot estabilizar
    await asyncio.sleep(5)
    
    while True:
        try:
            await asyncio.sleep(1)
            
            # Busca ordens ativas na API
            # Usa token_id para filtrar apenas o mercado atual
            active_orders = []
            
            if maker.token_id_yes:
                try:
                    params = OpenOrderParams(asset_id=maker.token_id_yes)
                    orders_yes = await asyncio.to_thread(
                        maker.client.get_orders,
                        params
                    )
                    if orders_yes:
                        active_orders.extend(orders_yes)
                except Exception as e:
                    aprint(f"⚠️ Auditor: erro ao buscar ordens YES: {e}")
            
            if maker.token_id_no:
                try:
                    params = OpenOrderParams(asset_id=maker.token_id_no)
                    orders_no = await asyncio.to_thread(
                        maker.client.get_orders,
                        params
                    )
                    if orders_no:
                        active_orders.extend(orders_no)
                except Exception as e:
                    aprint(f"⚠️ Auditor: erro ao buscar ordens NO: {e}")
            
            if not active_orders:
                continue
            
            # IDs das ordens que conhecemos como ativas
            known_active = set()
            if maker.bid_order.state == OrderState.ACTIVE and maker.bid_order.order_id:
                known_active.add(maker.bid_order.order_id)
            if maker.ask_order.state == OrderState.ACTIVE and maker.ask_order.order_id:
                known_active.add(maker.ask_order.order_id)
            
            # Verifica cada ordem ativa na API
            for order in active_orders:
                order_id = order.get('id') or order.get('order_id') or order.get('orderID')
                if not order_id:
                    continue
                
                # Se está ativa na API mas não conhecemos, é órfã!
                if order_id not in known_active:
                    price = order.get('price', 'N/A')
                    size = order.get('size', 'N/A')
                    side = order.get('side', 'N/A')
                    
                    aprint(f"🚨 ORDEM ÓRFÃ DETECTADA: {order_id[:16]}... side={side} price={price} size={size}")
                    maker.stats["orphan_orders_found"] += 1
                    
                    # Tenta cancelar a ordem órfã
                    try:
                        aprint(f"🗑️ Cancelando ordem órfã {order_id[:16]}...")
                        success, reason = await asyncio.to_thread(
                            maker._cancel_order_sync,
                            order_id
                        )
                        if success:
                            aprint(f"✅ Ordem órfã cancelada: {order_id[:16]}...")
                        else:
                            aprint(f"❌ Falha ao cancelar órfã: {order_id[:16]}... reason={reason}")
                    except Exception as e:
                        aprint(f"❌ Exception ao cancelar órfã: {e}")
        
        except asyncio.CancelledError:
            break
        except Exception as e:
            aprint(f"❌ Erro no auditor: {e}")
            await asyncio.sleep(1)


async def queue_monitor():
    """
    Monitora o tamanho da fila de prints.
    Alerta se a fila estiver ficando grande (indica que o sistema está lento).
    """
    global _print_queue
    
    while True:
        await asyncio.sleep(2)
        
        if _print_queue is not None:
            qsize = _print_queue.qsize()
            if qsize > 50:
                # Usa print direto para garantir que aparece
                print(f"⚠️ [QUEUE WARNING] Print queue size: {qsize} (>50 = lento!)")
            elif qsize > 100:
                print(f"🚨 [QUEUE CRITICAL] Print queue size: {qsize} (>100 = muito lento!)")


async def status_printer(maker: MarketMakerV2):
    """Imprime status periodicamente"""
    while True:
        await asyncio.sleep(5)
        
        if maker.price_pred is not None:
            tm_seconds = time.time() % 900
            t = tm_seconds / 60
            pos = maker.position
            
            can_trade = "✅" if maker._can_trade() else "❌"
            
            # Status das ordens (com info de partial fill)
            if maker.bid_order.state == OrderState.ACTIVE:
                if maker.bid_order.filled_size > 0:
                    bid_status = f"BID@{maker.bid_order.price:.2f}({maker.bid_order.filled_size:.0f}/{maker.bid_order.size:.0f})"
                else:
                    bid_status = f"BID@{maker.bid_order.price:.2f}"
            else:
                bid_status = "---"
            
            if maker.ask_order.state == OrderState.ACTIVE:
                if maker.ask_order.filled_size > 0:
                    ask_status = f"ASK@{maker.ask_order.price:.2f}({maker.ask_order.filled_size:.0f}/{maker.ask_order.size:.0f})"
                else:
                    ask_status = f"ASK@{maker.ask_order.price:.2f}"
            else:
                ask_status = "---"
            
            # Preços desejados
            desired_bid = maker.calculate_bid_price()
            desired_ask = maker.calculate_ask_price()
            desired_bid_str = f"{desired_bid:.2f}" if desired_bid else "N/A"
            desired_ask_str = f"{desired_ask:.2f}" if desired_ask else "N/A"
            
            # Bid/ask do mercado
            mkt_bid_str = f"{maker.best_bid:.2f}" if maker.best_bid else "N/A"
            mkt_ask_str = f"{maker.best_ask:.2f}" if maker.best_ask else "N/A"
            
            mkt_mid = (maker.best_bid + maker.best_ask) / 2 if maker.best_bid and maker.best_ask else None
            mkt_pnl = maker.pnl + (maker.pos_yes * mkt_mid + maker.pos_no * (1 - mkt_mid)) if mkt_mid is not None else 0
            
            # Tamanho da fila de prints
            queue_size = _print_queue.qsize() if _print_queue else 0
            queue_str = f"q={queue_size}" if queue_size < 20 else f"⚠️q={queue_size}"
            
            # Stats resumidas
            stats_str = f"ord:{maker.stats['orders_created']}/can:{maker.stats['orders_cancelled']}/fill:{maker.stats['orders_filled']}"
            if maker.stats['orphan_orders_found'] > 0:
                stats_str += f"/🚨orph:{maker.stats['orphan_orders_found']}"

            aprint(
                f"📊 pred={maker.price_pred:.4f} | "
                f"mkt: {mkt_bid_str}/{mkt_ask_str} | "
                f"quotes: {bid_status}/{ask_status} | "
                f"desired: {desired_bid_str}/{desired_ask_str} | "
                f"pos: {maker.pos_yes:.1f}Y/{maker.pos_no:.1f}N ({pos:+.1f}) | "
                f"pnl={mkt_pnl:.2f} | "
                f"t={t:.1f}min {can_trade} | "
                f"{stats_str} | {queue_str}"
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
    print(f"   - Cancel BID/ASK se |price - desired| > {SPREAD_CANCEL}")
    print(f"   - Cancel BID se price < best_bid - {MAX_DISTANCE}")
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
    
    print("\n🧹 Cancelando TODAS as ordens abertas (limpeza inicial)...")
    try:
        maker.cancel_all_open_orders()
        print("✅ Limpeza inicial concluída!")
    except Exception as e:
        print(f"⚠️ Erro na limpeza inicial: {e}")
    
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
        asyncio.create_task(_print_worker()),  # Print assíncrono (deve ser o primeiro!)
        asyncio.create_task(queue_monitor()),  # Monitora tamanho das filas
        asyncio.create_task(read_price_pred(zmq_context, maker)),
        asyncio.create_task(read_bid_ask(zmq_context, maker)),
        asyncio.create_task(read_price_15(zmq_context, maker)),
        asyncio.create_task(read_fills(zmq_context, maker)),
        asyncio.create_task(maker.fiscalize_loop()),  # Loop de fiscalização
        asyncio.create_task(order_auditor(maker)),  # Auditor de ordens órfãs
        asyncio.create_task(status_printer(maker)),
    ]
    
    try:
        await asyncio.gather(*tasks)
    except KeyboardInterrupt:
        aprint("\n👋 Encerrando...")
    finally:
        aprint("🗑️ Cancelando ordens pendentes...")
        maker._cancel_all_orders_sync()
        
        # Log final de estatísticas
        aprint(f"📈 STATS FINAIS: {maker.stats}")
        aprint(f"📋 Ordens no histórico: {len(maker.order_history)}")
        
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        zmq_context.term()
        print("✅ Encerrado!")  # Este pode ser print normal pois é o último


if __name__ == "__main__":
    asyncio.run(main())
