import { RealTimeDataClient } from "../src/client";
import { Message } from "../src/model";
import * as zmq from "zeromq";
import * as fs from "fs";
import * as path from "path";
import WebSocket from "ws";

const ZMQ_ENDPOINT = "ipc:///tmp/chainlink_btc.ipc";
const BINANCE_URL = "wss://stream.binance.com:9443/ws/btcusdt@aggTrade";
const HISTORY_FILE = path.join(__dirname, "../../data/btc_price_history.jsonl");
const FLUSH_INTERVAL_MS = 5000; // Escreve a cada 5 segundos
const BATCH_SIZE = 100; // Ou quando tiver 100 registros

// Formato unificado para IPC
// Quando é chainlink: binance_price = -1, binance_qty = -1
// Quando é binance: chainlink_price = -1
interface PriceMessage {
    ts: number;
    chainlink_price: number;
    binance_price: number;
    binance_qty: number;
}

async function main() {
    // Socket ZeroMQ (PUSH)
    const sock = new zmq.Push();
    await sock.bind(ZMQ_ENDPOINT);
    console.log(`✅ ZeroMQ socket bound to ${ZMQ_ENDPOINT}`);

    // Buffer para escrita em batch (zero impacto no IPC)
    let buffer: string[] = [];
    let writeStream = fs.createWriteStream(HISTORY_FILE, { flags: "a" });
    
    // Controle de duplicação Chainlink
    let lastChainlinkPrice: number | null = null;
    let lastChainlinkTimestamp: number | null = null;

    // Flush automático por tempo
    const flushInterval = setInterval(() => {
        if (buffer.length > 0) {
            writeStream.write(buffer.join("\n") + "\n");
            buffer = [];
        }
    }, FLUSH_INTERVAL_MS);

    // Função para enviar via IPC
    function sendIPC(data: PriceMessage, saveToHistory: boolean = false) {
        try {
            sock.send(JSON.stringify(data));
        } catch (err: any) {
            if (err.code !== 'EAGAIN' && err.code !== 'EBUSY') {
                console.error('❌ Error sending via IPC:', err);
            }
        }

        // Adiciona ao buffer para histórico (apenas Chainlink)
        if (saveToHistory) {
            buffer.push(JSON.stringify({ ts: data.ts, price: data.chainlink_price }));

            // Flush se buffer ficar grande
            if (buffer.length >= BATCH_SIZE) {
                setImmediate(() => {
                    writeStream.write(buffer.join("\n") + "\n");
                    buffer = [];
                });
            }
        }
    }

    // ==================== BINANCE ====================
    function setupBinance(): WebSocket {
        const ws = new WebSocket(BINANCE_URL);

        ws.on("open", () => {
            console.log("🟡 Binance: Connected to aggTrade stream");
        });

        ws.on("message", (rawData: WebSocket.Data) => {
            const msg = JSON.parse(rawData.toString());

            const price = parseFloat(msg.p);
            const qty = parseFloat(msg.q);
            const tsSource = msg.T; // Timestamp da trade (ms)

            const data: PriceMessage = {
                ts: tsSource,
                chainlink_price: -1,
                binance_price: price,
                binance_qty: qty,
            };

            // console.log(`� Binance: $${price.toFixed(2)} | qty: ${qty}`);
            sendIPC(data);
        });

        ws.on("error", (err: Error) => {
            console.error("🟡 Binance Error:", err.message);
        });

        ws.on("close", () => {
            console.log("🟡 Binance: Disconnected, reconnecting in 5s...");
            setTimeout(setupBinance, 5000);
        });

        return ws;
    }

    // ==================== CHAINLINK ====================
    const onMessage = (_: RealTimeDataClient, message: Message): void => {
        const payload = message.payload as any;

        // Segurança mínima
        if (!payload || payload.value == null) {
            return;
        }

        const price = Number(payload.value);
        const ts = payload.timestamp;
        
        // Ignora duplicatas exatas (mesmo preço E mesmo timestamp)
        if (price === lastChainlinkPrice && ts === lastChainlinkTimestamp) {
            return;
        }
        
        lastChainlinkPrice = price;
        lastChainlinkTimestamp = ts;

        // Chainlink timestamp pode estar em segundos, normaliza para ms
        const tsMs = ts > 1e12 ? ts : ts * 1000;

        const data: PriceMessage = {
            ts: tsMs,
            chainlink_price: price,
            binance_price: -1,
            binance_qty: -1,
        };

        console.log(`🔵 Chainlink: $${price.toFixed(2)} at ${Math.floor(ts)}`);
        sendIPC(data, true);  // true = salva no histórico
    };

    const onConnect = (client: RealTimeDataClient): void => {
        console.log("� Chainlink: Connected to Polymarket Real-Time Data");
        client.subscribe({
            subscriptions: [
                {
                    topic: "crypto_prices_chainlink",
                    type: "*",
                    filters: "{\"symbol\":\"btc/usd\"}",
                },
            ],
        });
        console.log("� Chainlink: Subscribed to BTC/USD feed");
    };

    // Variável para guardar referência do WebSocket Binance
    let binanceWs: WebSocket | null = null;

    process.on("SIGINT", async () => {
        console.log("\n👋 Closing connections...");
        
        // Flush final antes de fechar
        if (buffer.length > 0) {
            writeStream.write(buffer.join("\n") + "\n");
        }
        
        clearInterval(flushInterval);
        writeStream.end();
        
        // Fecha Binance WebSocket
        if (binanceWs) {
            binanceWs.close();
        }
        
        await sock.close();
        process.exit(0);
    });

    // Inicia ambas as conexões
    console.log("🚀 Starting combined BTC price stream (Chainlink + Binance)...");
    binanceWs = setupBinance();
    new RealTimeDataClient({ onConnect, onMessage }).connect();
}

main();