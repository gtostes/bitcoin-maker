#!/usr/bin/env python3
# 🔧 Stream de Mercado Polymarket com IPC e Histórico (Zero Latência)

from websocket import WebSocketApp
import json
import time
import threading
import ssl
from utils import get_markets
import re
import zmq
import os
import argparse
from dotenv import load_dotenv
from py_clob_client.client import ClobClient
from py_clob_client.constants import POLYGON

load_dotenv()

MARKET_CHANNEL = "market"
USER_CHANNEL = "user"

# 🔧 Configurações IPC e Arquivo
ZMQ_MARKET_ENDPOINT = "ipc:///tmp/polymarket_market.ipc"
ZMQ_FILLS_ENDPOINT = "ipc:///tmp/polymarket_fills.ipc"  # Novo endpoint para fills
HISTORY_FILE = os.path.join(os.path.dirname(__file__), "data", "market_history.jsonl")
FLUSH_INTERVAL_SEC = 5  # Flush a cada 5 segundos
BATCH_SIZE = 100  # Ou quando tiver 100 registros


class WebSocketOrderBook:
    def __init__(self, channel_type, url, data, auth, message_callback, verbose, save_history=True):
        self.channel_type = channel_type
        self.url = url
        self.data = data
        self.auth = auth
        self.message_callback = message_callback
        self.verbose = verbose
        self.save_history = save_history
        self.size_matched = {}
        self.furl = url + "/ws/" + channel_type
        
        # 🔧 IPC ZeroMQ (PUSH)
        self.zmq_context = zmq.Context()
        self.zmq_socket = self.zmq_context.socket(zmq.PUSH)
        self.zmq_socket.bind(ZMQ_MARKET_ENDPOINT)
        print(f"✅ ZeroMQ socket bound to {ZMQ_MARKET_ENDPOINT}")
        
        # 🔧 Buffer para escrita em arquivo (zero impacto no IPC)
        self.buffer = []
        self.file_handle = None
        self.flush_timer = None
        
        if self.save_history:
            self.file_handle = open(HISTORY_FILE, "a")
            self._start_flush_timer()
            print(f"💾 History saving enabled: {HISTORY_FILE}")
        else:
            print(f"⚠️ History saving disabled")
        
        # 🔧 CONFIGURAÇÕES DE RECONEXÃO
        self.reconnect_enabled = True
        self.reconnect_delay = 1
        self.max_reconnect_delay = 60
        self.reconnect_attempts = 0
        self.max_reconnect_attempts = None
        self.is_running = False
        self._stop_event = threading.Event()
        
        self._create_websocket()
        self.orderbooks = {}

    def _start_flush_timer(self):
        """Timer automático para flush do buffer"""
        if not self.save_history:
            return
            
        def flush():
            if self.buffer and self.file_handle:
                self.file_handle.write("\n".join(self.buffer) + "\n")
                self.file_handle.flush()
                self.buffer = []
            if not self._stop_event.is_set() and self.save_history:
                self._start_flush_timer()
        
        self.flush_timer = threading.Timer(FLUSH_INTERVAL_SEC, flush)
        self.flush_timer.daemon = True
        self.flush_timer.start()

    def _create_websocket(self):
        """Cria uma nova instância do WebSocket"""
        self.ws = WebSocketApp(
            self.furl,
            on_message=self.on_message,
            on_error=self.on_error,
            on_close=self.on_close,
            on_open=self.on_open,
        )

    def on_message(self, ws, message):
        # Reset reconnect delay on successful message
        self.reconnect_delay = 1
        self.reconnect_attempts = 0
        
        # Ignorar PONG
        if message == "PONG":
            return
        
        try:
            # Parse JSON
            data = json.loads(message)
            
            # Processar baseado no tipo de evento
            event_type = data.get('event_type')
            
            if event_type == 'best_bid_ask':
                self.process_bid_ask(data)
                    
        except json.JSONDecodeError:
            print(f"❌ Failed to parse JSON: {message}")
        except Exception as e:
            print(f"❌ Error processing message: {e}")

    def process_bid_ask(self, data):
        """Processa bid/ask com prioridade para IPC"""
        # Extrair dados relevantes (completo para IPC)
        market_data = {
            'market': data['market'],
            'asset_id': data['asset_id'],
            'best_bid': data['best_bid'],
            'best_ask': data['best_ask'],
            'spread': data['spread'],
            'timestamp': data['timestamp'],
            'event_type': data['event_type']
        }
        
        # 1. PRIORIDADE: Envio IPC (não bloqueante, zero latência)
        try:
            self.zmq_socket.send_json(market_data, zmq.NOBLOCK)
            # Log simples de confirmação
            if not hasattr(self, '_first_send_logged'):
                print(f"✅ STREAMING: bid={data['best_bid']} | ask={data['best_ask']} | spread={data['spread']}")
                self._first_send_logged = True
        except zmq.Again:
            # Socket cheio, ignora (prioriza latência baixa)
            pass
        
        # 2. BACKGROUND: Dados simplificados para arquivo (apenas timestamp, bid, ask)
        if self.save_history:
            file_data = {
                'timestamp': data['timestamp'],
                'best_bid': data['best_bid'],
                'best_ask': data['best_ask']
            }
            self.buffer.append(json.dumps(file_data))
            
            # 3. Flush se buffer ficar grande (evita uso excessivo de memória)
            if len(self.buffer) >= BATCH_SIZE:
                self.file_handle.write("\n".join(self.buffer) + "\n")
                self.file_handle.flush()
                self.buffer = []

    def on_error(self, ws, error):
        print(f"❌ Error: {error}")

    def on_close(self, ws, close_status_code, close_msg):
        print(f"🔌 Connection closed (code: {close_status_code}, msg: {close_msg})")
        
        # 🔧 LÓGICA DE RECONEXÃO
        if self.reconnect_enabled and self.is_running and not self._stop_event.is_set():
            self._attempt_reconnect()

    def _attempt_reconnect(self):
        """Tenta reconectar com backoff exponencial"""
        if self.max_reconnect_attempts is not None and self.reconnect_attempts >= self.max_reconnect_attempts:
            print(f"❌ Máximo de tentativas de reconexão ({self.max_reconnect_attempts}) atingido. Parando.")
            self.is_running = False
            return
        
        self.reconnect_attempts += 1
        print(f"🔄 Tentativa de reconexão #{self.reconnect_attempts} em {self.reconnect_delay}s...")
        
        if self._stop_event.wait(self.reconnect_delay):
            return
        
        self.reconnect_delay = min(self.reconnect_delay * 2, self.max_reconnect_delay)
        
        self._create_websocket()
        self._run_websocket()

    def on_open(self, ws):
        print(f"✅ Conectado ao {self.channel_type} WebSocket!")
        self.reconnect_attempts = 0
        self.reconnect_delay = 1
        
        if self.channel_type == MARKET_CHANNEL:
            ws.send(json.dumps({"assets_ids": self.data, "type": MARKET_CHANNEL, "custom_feature_enabled": True}))
        elif self.channel_type == USER_CHANNEL and self.auth:
            ws.send(
                json.dumps(
                    {"markets": self.data, "type": USER_CHANNEL, "auth": self.auth}
                )
            )
        else:
            return

        thr = threading.Thread(target=self.ping, args=(ws,))
        thr.daemon = True
        thr.start()

    def subscribe_to_tokens_ids(self, assets_ids):
        if self.channel_type == MARKET_CHANNEL:
            self.ws.send(json.dumps({"assets_ids": assets_ids, "operation": "subscribe"}))

    def unsubscribe_to_tokens_ids(self, assets_ids):
        if self.channel_type == MARKET_CHANNEL:
            self.ws.send(json.dumps({"assets_ids": assets_ids, "operation": "unsubscribe"}))

    def ping(self, ws):
        while not self._stop_event.is_set():
            try:
                ws.send("PING")
                time.sleep(5)
            except Exception as e:
                print(f"❌ Ping error: {e}")
                break

    def _run_websocket(self):
        """Executa o WebSocket com configurações SSL"""
        ssl._create_default_https_context = ssl._create_unverified_context
        sslopt = {"cert_reqs": ssl.CERT_NONE}
        self.ws.run_forever(sslopt=sslopt)

    def run(self):
        """Inicia o WebSocket com reconexão automática"""
        self.is_running = True
        self._stop_event.clear()
        
        print(f"🔌 Conectando ao {self.channel_type} WebSocket...")
        print(f"♻️ Reconexão automática: {'Ativada' if self.reconnect_enabled else 'Desativada'}")
        
        self._run_websocket()

    def stop(self):
        """Para o WebSocket e desabilita reconexão"""
        print("🛑 Parando WebSocket...")
        self.is_running = False
        self._stop_event.set()
        
        # Flush final
        if self.save_history and self.buffer and self.file_handle:
            self.file_handle.write("\n".join(self.buffer) + "\n")
            self.file_handle.flush()
            self.file_handle.close()
        
        if self.flush_timer:
            self.flush_timer.cancel()
        
        self.ws.close()
        self.zmq_socket.close()
        self.zmq_context.term()
        print("✅ Recursos liberados")


class UserWebSocket:
    """WebSocket para User Channel - recebe fills/trades do usuário"""
    
    def __init__(self, url, auth, my_api_key, verbose=True):
        self.url = url + "/ws/user"
        self.auth = auth
        self.my_api_key = my_api_key  # Minha API key para identificar minhas ordens maker
        self.verbose = verbose
        
        # 🔧 IPC ZeroMQ (PUSH) para fills
        self.zmq_context = zmq.Context()
        self.zmq_socket = self.zmq_context.socket(zmq.PUSH)
        self.zmq_socket.bind(ZMQ_FILLS_ENDPOINT)
        print(f"✅ ZeroMQ fills socket bound to {ZMQ_FILLS_ENDPOINT}")
        
        # Reconexão
        self.reconnect_enabled = True
        self.reconnect_delay = 1
        self.max_reconnect_delay = 60
        self.is_running = False
        self._stop_event = threading.Event()
        
        self._create_websocket()
    
    def _create_websocket(self):
        self.ws = WebSocketApp(
            self.url,
            on_message=self.on_message,
            on_error=self.on_error,
            on_close=self.on_close,
            on_open=self.on_open,
        )
    
    def on_open(self, ws):
        print("🔌 User Channel conectado!")
        # Autenticar
        auth_msg = {"auth": self.auth, "type": "auth"}
        ws.send(json.dumps(auth_msg))
        print("🔑 Auth enviado ao User Channel")
    
    def on_message(self, ws, message):
        self.reconnect_delay = 1
        
        if message == "PONG":
            return
        
        try:
            data = json.loads(message)
            event_type = data.get('event_type')
            
            # Processar trades (fills)
            if event_type == 'trade':
                self.process_trade(data)
            elif event_type == 'order':
                self.process_order(data)
                
        except json.JSONDecodeError:
            print(f"❌ User: Failed to parse JSON: {message}")
        except Exception as e:
            print(f"❌ User: Error processing message: {e}")
    
    def process_trade(self, data):
        """Processa trade - verifica maker_orders para encontrar minhas ordens"""
        status = data.get('status', '')
        
        # Só processar trades confirmados
        if status not in ['MATCHED']:
            return
        
        # Verificar maker_orders para encontrar minhas ordens
        maker_orders = data.get('maker_orders', [])
        
        # Processa cada maker_order separadamente (para ter order_id correto)
        for maker_order in maker_orders:
            owner = maker_order.get('owner', '')
            
            # Verificar se esta ordem maker é minha
            if owner == self.my_api_key:
                matched = float(maker_order.get('matched_amount', 0))
                if matched <= 0:
                    continue
                    
                my_price = float(maker_order.get('price', 0))
                my_outcome = maker_order.get('outcome')
                my_order_id = maker_order.get('order_id')  # ORDER ID da minha ordem!
                
                # Maker side: se eu sou maker e alguém comprou, eu estava vendendo e vice-versa
                # O trade.side é do taker, então meu side é o oposto
                taker_side = data.get('side', '')
                taker_outcome = data.get('outcome', '')

                if taker_outcome != my_outcome:
                    my_side = taker_side
                else:
                    my_side = 'SELL' if taker_side == 'BUY' else 'BUY'
                
                fill_data = {
                    'event_type': 'fill',
                    'status': status,
                    'side': my_side,              # Meu lado (oposto do taker)
                    'outcome': my_outcome,         # YES ou NO
                    'size': matched,               # Quantidade que foi matched DESTA ordem
                    'price': my_price,
                    'asset_id': data.get('asset_id'),
                    'timestamp': data.get('timestamp'),
                    'is_maker': True,              # Indica que fui maker
                    'order_id': my_order_id,       # ID da minha ordem maker!
                }
                
                # Enviar via IPC
                self.zmq_socket.send_json(fill_data)
                
                if self.verbose:
                    order_id_short = my_order_id[:16] if my_order_id else "N/A"
                    print(f"💰 MAKER FILL: {my_side} {matched} {my_outcome} @ {my_price} [{status}] order={order_id_short}...")
        
        # Também verificar se EU sou o taker (trade_owner == my_api_key)
        trade_owner = data.get('trade_owner', '')
        if trade_owner == self.my_api_key:
            taker_fill = {
                'event_type': 'fill',
                'status': status,
                'side': data.get('side'),           # BUY ou SELL
                'outcome': data.get('outcome'),     # YES ou NO
                'size': float(data.get('size', 0)),
                'price': float(data.get('price', 0)),
                'asset_id': data.get('asset_id'),
                'timestamp': data.get('timestamp'),
                'is_maker': False,                  # Indica que fui taker
                'order_id': data.get('taker_order_id'),  # ID da minha ordem taker
            }
            
            self.zmq_socket.send_json(taker_fill)
            
            if self.verbose:
                order_id_short = taker_fill['order_id'][:16] if taker_fill['order_id'] else "N/A"
                print(f"💰 TAKER FILL: {taker_fill['side']} {taker_fill['size']} {taker_fill['outcome']} @ {taker_fill['price']} [{status}] order={order_id_short}")
    
    def process_order(self, data):
        """Processa atualizações de ordem (opcional, para debug)"""
        if self.verbose:
            status = data.get('status', '')
            print(f"📋 Order update: {status}")
    
    def on_error(self, ws, error):
        print(f"❌ User Channel error: {error}")
    
    def on_close(self, ws, close_status_code, close_msg):
        print(f"🔌 User Channel fechado: {close_status_code} - {close_msg}")
        
        if self.is_running and self.reconnect_enabled and not self._stop_event.is_set():
            print(f"♻️ Reconectando User Channel em {self.reconnect_delay}s...")
            time.sleep(self.reconnect_delay)
            self.reconnect_delay = min(self.reconnect_delay * 2, self.max_reconnect_delay)
            self._create_websocket()
            self._run_websocket()
    
    def _run_websocket(self):
        self.ws.run_forever(
            ping_interval=30,
            ping_timeout=10,
            sslopt={"cert_reqs": ssl.CERT_NONE}
        )
    
    def run(self):
        self.is_running = True
        self._stop_event.clear()
        print(f"🔌 Conectando ao User Channel...")
        self._run_websocket()
    
    def stop(self):
        print("🛑 Parando User Channel...")
        self.is_running = False
        self._stop_event.set()
        self.ws.close()
        self.zmq_socket.close()
        self.zmq_context.term()
        print("✅ User Channel recursos liberados")


def main():
    """Loop principal que gerencia mercados de 15 minutos"""
    # 🔧 Parse argumentos de linha de comando
    parser = argparse.ArgumentParser(description='Stream de Mercado Polymarket com IPC')
    parser.add_argument('--save-history', type=int, default=1, choices=[0, 1],
                        help='Salvar histórico em arquivo (0=não, 1=sim, padrão=1)')
    args = parser.parse_args()
    
    SAVE_HISTORY = bool(args.save_history)
    
    print("🚀 Iniciando Stream de Mercado Polymarket com IPC")
    print(f"📡 IPC: {ZMQ_MARKET_ENDPOINT}")
    if SAVE_HISTORY:
        print(f"💾 Histórico: {HISTORY_FILE}\n")
    else:
        print(f"⚠️ Histórico desabilitado\n")
    
    # Autenticação Polymarket
    host = "https://clob.polymarket.com"
    key = os.getenv("PK")
    proxy_address = os.getenv("BROWSER_ADDRESS")
    client = ClobClient(host, key=key, chain_id=POLYGON, signature_type=1, funder=proxy_address)
    client.set_api_creds(client.create_or_derive_api_creds())
    
    keys = client.derive_api_key()

    url = "wss://ws-subscriptions-clob.polymarket.com"
    api_key = keys.api_key
    api_secret = keys.api_secret
    api_passphrase = keys.api_passphrase

    auth = {"apiKey": api_key, "secret": api_secret, "passphrase": api_passphrase}

    market_connection = None
    user_connection = None

    # 🔧 Iniciar User Channel (persiste durante toda execução)
    # Passa api_key para identificar minhas ordens maker nos trades
    user_connection = UserWebSocket(url, auth, my_api_key=api_key, verbose=True)
    user_thread = threading.Thread(target=user_connection.run)
    user_thread.daemon = True
    user_thread.start()
    print(f"📡 User Channel IPC: {ZMQ_FILLS_ENDPOINT}\n")

    try:
        while True:
            time_market = (time.time() // 900)
            id_time = 900 * int(time_market)

            markets = get_markets(f'https://polymarket.com/event/btc-updown-15m-{id_time}')
            ids = re.findall(r'"(\d+)"', markets[0]['clobTokenIds'])

            assets_ids = ids[:1]

            # Para conexão anterior se existir
            if market_connection:
                market_connection.stop()

            # Nova conexão
            market_connection = WebSocketOrderBook(
                MARKET_CHANNEL, url, assets_ids, auth, None, True, save_history=SAVE_HISTORY
            )

            # 🔧 Executa em thread separada
            ws_thread = threading.Thread(target=market_connection.run)
            ws_thread.daemon = True
            ws_thread.start()

            now_ts = time.time()
            target_ts = id_time + 900

            sleep_seconds = max(0, target_ts - now_ts)
            print(f'⏰ Going to sleep for {sleep_seconds:.1f}s until next market\n')
            time.sleep(sleep_seconds)
            print("✅ New WS loop!\n")

    except KeyboardInterrupt:
        print("\n👋 Encerrando...")
        if market_connection:
            market_connection.stop()
        if user_connection:
            user_connection.stop()


if __name__ == "__main__":
    main()
