#!/usr/bin/env python3
# 🔧 Processamento de Preço BTC - Apenas Modelo 2 (Binance Features)

import json
from collections import deque
import numpy as np
import math
import lightgbm as lgb
import zmq
import os
import time

# 🔧 Configurações IPC
IPC_INPUT = "ipc:///tmp/chainlink_btc.ipc"  # Recebe preços BTC
IPC_OUTPUT = "ipc:///tmp/btc_price_pred.ipc"  # Envia predições
IPC_PRICE15 = "ipc:///tmp/btc_price_15.ipc"  # Envia preço do início do período 15min
HISTORY_FILE = os.path.join(os.path.dirname(__file__), "data", "btc_price_history.jsonl")
MODEL_FILE = "models/lgb_btc_0201.txt"

# Carrega modelo 2 apenas
model = lgb.Booster(model_file=MODEL_FILE)
print(f"✅ Modelo LightGBM carregado: {MODEL_FILE}")


class get_past_prices():
    def __init__(self):
        self.dic = {}
        self.tm = -1

    def insert(self, ts, price):
        ts = (ts//1000)*1000 + 1000
        self.dic[ts] = price
        if len(self.dic) > 10:
            self.dic.pop(min(self.dic.keys()))
    
    def get(self, ts):
        return self.dic.get(ts, -1)


class myq():
    def __init__(self):
        self.vol_10s = deque()
        self.vwap_dq = deque()

        self.qty_10s = 0
        self.vwap = 0
        self.tm = 0

        self.price_vwap = 0

        self.pricevol = 0
        self.vol = 0

        self.past_prices = get_past_prices()

        self.last_chainlink = -1
        self.last_binance = -1

        self.predict = None
        self.predict_kalman = None


    def update(self, ts, qty, price):
        self.vol_10s.append((ts, qty))
        self.qty_10s += qty
        self.vwap_dq.append((ts, qty, price))
        self.pricevol += qty*price
        self.vol += qty

        while self.vol_10s and self.vol_10s[0][0] < ts - 10000:
            old_ts, old_qty = self.vol_10s.popleft()
            self.qty_10s -= old_qty

        vol_1s = self.qty_10s/10

        while len(self.vwap_dq) > 1 and (self.vwap_dq[0][0] < ts - 1500 or self.vol > vol_1s):
            old_ts, old_qty, old_price = self.vwap_dq.popleft()
            self.pricevol -= old_qty*old_price
            self.vol -= old_qty
            
        self.vwap = self.pricevol / self.vol if self.vol > 0 else 0
        self.tm = ts
        self.past_prices.insert(ts, self.vwap)

    
    def insert_chainlink(self, ts, price):
        self.last_chainlink = price
        self.last_binance = self.past_prices.get(ts)

    def get_prediction(self):
        if self.last_binance == -1: 
            return self.last_chainlink
        else: 
            return self.last_chainlink + (self.vwap - self.last_binance)*0.7
        
        
class price_pred_model2:
    """
    Modelo 2: Features baseadas em Binance (VWAP)
    Features: vol_ewma_60, vol_vol_ewma_60, vol_regime, distance_15, rsi_60, time_sin, time_cos, Q
    """
    def __init__(self):
        self.xgb = {
            "vol_ewma_60": 0, "vol_vol_ewma_60": 0, "vol_regime": 0, "distance_15": 0,
            "rsi_60": 0, "time_sin": 0, "time_cos": 0, "Q": 0,
        }

        self.t = 15
        self.vol_ewma_60 = 0
        self.vol_ewma_120 = 0
        self.vol_vol_ewma_60 = 0
        self.price_ewma_15 = 0

        self.gain_60 = 0
        self.loss_60 = 0

        self.last_tm = None
        self.price_15 = None
        self.price_1min = deque()

    def update_features(self, price, t):
        self.price_1min.append((t, price))
        while self.price_1min and self.price_1min[0][0] < t - 60000:
            self.price_1min.popleft()
        
        if self.last_tm is not None:
            dt = t - self.last_tm
            dp = np.log(price + 1e-18) - np.log(self.price_1min[0][1] + 1e-18)
            assert (
                price + 1e-18 > 0 and self.price_1min[0][1] + 1e-18 > 0
            ), f"assert falhou: price={price}, price_1min={self.price_1min[0][1]}"
            alpha_15 = -np.log(14/16)/(60*1000)
            alpha_60 = -np.log(59/61)/(60*1000)
            alpha_120 = -np.log(119/121)/(60*1000)

            save_vol_ewma_60 = self.vol_ewma_60
            self.vol_ewma_60 = self.vol_ewma_60 * np.exp(-alpha_60*dt) + (dp**2) * (1 - np.exp(-alpha_60*dt))
            self.vol_ewma_120 = self.vol_ewma_120 * np.exp(-alpha_120*dt) + (dp**2) * (1 - np.exp(-alpha_120*dt))
            self.xgb['vol_ewma_60'] = np.log(self.vol_ewma_60+1e-18)

            self.vol_vol_ewma_60 = self.vol_vol_ewma_60 * np.exp(-alpha_60*dt) + ((np.log(self.vol_ewma_60 + 1e-18) - np.log(save_vol_ewma_60 + 1e-18))**2) * (1 - np.exp(-alpha_60*dt))
            self.xgb['vol_vol_ewma_60'] = self.vol_vol_ewma_60
            
            self.xgb['vol_regime'] = np.sqrt(self.vol_ewma_60 / self.vol_ewma_120)

            self.price_ewma_15 = self.price_ewma_15 * np.exp(-alpha_15*dt) + price * (1 - np.exp(-alpha_15*dt))
            self.xgb['distance_15'] = (price - self.price_ewma_15) / (self.price_ewma_15 + 1e-9)

            self.gain_60 = self.gain_60 * np.exp(-alpha_60*dt) + max(dp, 0) * (1 - np.exp(-alpha_60*dt))
            self.loss_60 = self.loss_60 * np.exp(-alpha_60*dt) + max(-dp, 0) * (1 - np.exp(-alpha_60*dt))
            rs = self.gain_60 / (self.loss_60 + 1e-9)
            self.xgb['rsi_60'] = 100 - (100 / (1 + rs))
            time_in_minutes = (int(time.time() * 1000) // 60000) % 1440
            self.xgb['time_sin'] = math.sin(2 * math.pi * time_in_minutes / 1440)
            self.xgb['time_cos'] = math.cos(2 * math.pi * time_in_minutes / 1440)

            self.t = 15 - 15 * (t / (60000 * 15) % 1)
            if self.price_15 is not None and self.vol_ewma_60 > 0:
                self.xgb['Q'] = np.log(self.price_15/price) / np.sqrt(self.vol_ewma_60 * self.t)
            else:
                self.xgb['Q'] = 0

        self.last_tm = t
        

    def update_price15(self, price_15):
        self.price_15 = price_15
    
    def get_prediction(self):
        """Retorna predição do modelo 2"""
        entry = [list(self.xgb.values())]
        return model.predict(entry)[0]
    
    def get_prediction_with_Q(self, Q_value):
        """Retorna predição do modelo 2 com um valor específico de Q"""
        features = self.xgb.copy()
        features['Q'] = Q_value
        entry = [list(features.values())]
        return model.predict(entry)[0]
    
    def get_prediction_and_dQ(self):
        """
        Retorna (pred, dpred_dQ) onde dpred_dQ é a derivada parcial em relação a Q.
        Estimada via diferenças finitas: [f(Q+0.01) - f(Q-0.01)] / 0.02
        """
        Q = self.xgb['Q']
        delta = 0.05
        
        pred = self.get_prediction()
        pred_plus = self.get_prediction_with_Q(Q + delta)
        pred_minus = self.get_prediction_with_Q(Q - delta)
        
        dpred_dQ = (pred_plus - pred_minus) / (2 * delta)
        
        return pred, dpred_dQ


class BTCPriceProcessor:
    def __init__(self):
        self.minute_15 = -1
        self.t = 15
        self.price_15 = None

        # Instância da classe myq para processamento de preços Binance
        self.upd_vwap = myq()
        
        # Instância do modelo 2 (apenas)
        self.model2 = price_pred_model2()
        
        # 🔧 IPC ZeroMQ
        self.zmq_context = zmq.Context()
        
        # Socket PULL para receber preços
        self.sock_input = self.zmq_context.socket(zmq.PULL)
        self.sock_input.connect(IPC_INPUT)
        print(f"✅ Conectado ao IPC de entrada: {IPC_INPUT}")
        
        # Socket PUSH para enviar predições
        self.sock_output = self.zmq_context.socket(zmq.PUSH)
        self.sock_output.bind(IPC_OUTPUT)
        print(f"✅ IPC de saída criado: {IPC_OUTPUT}")
        
        # Socket PUB para enviar price_15 (broadcast para múltiplos consumidores)
        self.sock_price15 = self.zmq_context.socket(zmq.PUB)
        self.sock_price15.bind(IPC_PRICE15)
        print(f"✅ IPC price_15 criado: {IPC_PRICE15} (PUB)")
        
        # Inicializa com histórico
        self.load_history()
        self.price_15 = self.model2.price_15

        
    def load_history(self):
        """Carrega histórico do arquivo JSONL (últimos 10K pontos)"""
        print(f"📂 Carregando histórico de: {HISTORY_FILE}")
        
        if not os.path.exists(HISTORY_FILE):
            print("⚠️ Arquivo de histórico não encontrado. Iniciando do zero.")
            return
        
        timestamps = []
        prices = []
        
        with open(HISTORY_FILE, "r") as f:
            for line in f:
                if not line.strip():
                    continue
                
                try:
                    data = json.loads(line)
                    ts = data["ts"]
                    price = data["price"]
                    timestamps.append(ts)
                    prices.append(price)
                except Exception as e:
                    continue

        print(f"📊 {len(prices)} registros históricos")
        
        # Usar apenas os últimos 20K pontos
        if len(prices) > 20000:
            timestamps = timestamps[-20000:]
            prices = prices[-20000:]

        print(f"📊 Usando {len(prices)} registros históricos")
    
        
        if len(prices) == 0:
            return
        
        # Passa o histórico para o modelo 2
        for i in range(len(prices)):
            ts_feed = timestamps[i]
            price = prices[i]

            if ts_feed % (1000*60*15) == 0:
                self.price_15 = price
                self.model2.update_price15(self.price_15)

            if self.price_15 is not None:
                self.model2.update_features(price, ts_feed)
        
        print(f"✅ Histórico processado.")
        print(f"   Model2 features: {self.model2.xgb}")
    
    def run(self):
        """Loop principal que escuta IPC"""
        print("🎧 Escutando preços BTC via IPC (Modelo 2 apenas)...\n")
        
        pred_model2 = None
        dpred_dQ = None
        last_binance_second = -1  # Para controlar print de apenas 1 por segundo
        
        try:
            while True:
                # Recebe mensagem do IPC (novo formato com chainlink + binance)
                message = self.sock_input.recv_json()
                
                ts_feed = message["ts"]
                chainlink_price = message["chainlink_price"]
                binance_price = message["binance_price"]
                binance_qty = message["binance_qty"]

                price_pred_binance = None
                
                # ========== BINANCE: Atualiza upd_vwap.update() e recalcula pred_model2 ==========
                if binance_price != -1:
                    # Atualiza myq com trade do Binance
                    self.upd_vwap.update(ts_feed, binance_qty, binance_price)
                    
                    # Obtém preço predito da Binance e atualiza modelo 2
                    price_pred_binance = self.upd_vwap.get_prediction()
                    if price_pred_binance != -1:
                        self.model2.update_features(price_pred_binance, ts_feed)
                        pred_model2, dpred_dQ = self.model2.get_prediction_and_dQ()
                
                # ========== CHAINLINK: Atualiza upd_vwap.insert_chainlink() ==========
                if chainlink_price != -1:
                    # Atualiza diff no myq
                    self.upd_vwap.insert_chainlink(ts_feed, chainlink_price)
                    
                    # Atualiza price_15
                    minute_15_curr = ts_feed // (60000 * 15)
                    if minute_15_curr != self.minute_15:
                        self.price_15 = chainlink_price
                        self.minute_15 = minute_15_curr
                        self.model2.update_price15(chainlink_price)
                        print(f"🆕 Novo período 15m: price_15={self.price_15}")
                
                # Calcula t
                self.t = 15 - 15 * (ts_feed / (60000 * 15) % 1)
                
                # Determina preço raw para referência
                price_raw = chainlink_price if chainlink_price != -1 else price_pred_binance
                
                # 🔧 Envia predição via IPC de saída (apenas modelo 2)
                output_data = {
                    "price": price_raw,
                    "pred_model2": pred_model2,
                    "dpred_dQ": dpred_dQ,
                    "timestamp": ts_feed,
                    "features_model2": self.model2.xgb.copy()
                }
                
                try:
                    self.sock_output.send_json(output_data, zmq.NOBLOCK)
                except zmq.Again:
                    pass  # Socket cheio, ignora
                
                # Envia price_15
                if self.price_15 is not None:
                    price15_data = {
                        "price_15": self.price_15,
                        "timestamp": ts_feed,
                        "period": self.minute_15
                    }
                    try:
                        self.sock_price15.send_json(price15_data, zmq.NOBLOCK)
                    except zmq.Again:
                        pass
                
                # Log
                src = "🔵CL" if chainlink_price != -1 else "🟡BN"
                pred2_str = f"{pred_model2:.4f}" if pred_model2 is not None else "N/A"
                dpred_dQ_str = f"{dpred_dQ:.6f}" if dpred_dQ is not None else "N/A"
                
                # Para Binance, printa apenas o primeiro de cada segundo
                current_second = ts_feed // 1000
                should_print = False
                
                if chainlink_price != -1:
                    # Chainlink: sempre printa
                    should_print = True
                elif current_second != last_binance_second:
                    # Binance: apenas primeiro de cada segundo
                    should_print = True
                    last_binance_second = current_second
                
                if should_print:
                    # Formata features do modelo 2
                    f2 = self.model2.xgb
                    f2_str = f"vol60={f2['vol_ewma_60']:.2f} vvol={f2['vol_vol_ewma_60']:.4f} regime={f2['vol_regime']:.3f} dist15={f2['distance_15']:.6f} rsi={f2['rsi_60']:.2f} Q={f2['Q']:.3f}"
                    
                    print(
                        f"{src} | ts={ts_feed} | price={price_raw:.2f} | pred2={pred2_str} | dpred_dQ={dpred_dQ_str}\n"
                        f"     M2: {f2_str}"
                    )
                
        except KeyboardInterrupt:
            print("\n👋 Encerrando...")
        finally:
            self.sock_input.close()
            self.sock_output.close()
            self.sock_price15.close()
            self.zmq_context.term()


if __name__ == "__main__":
    processor = BTCPriceProcessor()
    processor.run()
