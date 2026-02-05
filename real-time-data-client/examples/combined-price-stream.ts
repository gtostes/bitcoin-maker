import { RealTimeDataClient } from "../src/client";
import { Message } from "../src/model";
import WebSocket from "ws";
import * as fs from "fs";
import * as path from "path";

// Configurações
const BINANCE_URL = "wss://stream.binance.com:9443/ws/btcusdt@aggTrade";
const HISTORY_FILE = path.join(__dirname, "../../data/btc_combined_prices.jsonl");
const FLUSH_INTERVAL_MS = 5000;
const BATCH_SIZE = 100;

interface PriceRecord {
    source: "chainlink" | "binance";
    price: number;
    ts_source: number;      // Timestamp da fonte (exchange/chainlink)
    ts_received: number;    // Timestamp de quando recebemos
    latency_ms?: number;    // Diferença entre recebido e fonte
    extra?: {
        qty?: number;       // Quantidade (Binance)
        side?: string;      // BUY/SELL (Binance)
    };
}

// Buffer para escrita
let buffer: string[] = [];
const writeStream = fs.createWriteStream(HISTORY_FILE, { flags: "a" });

// Controle de duplicação Chainlink
let lastChainlinkPrice: number | null = null;
let lastChainlinkTs: number | null = null;

// Flush automático por tempo
const flushInterval = setInterval(() => {
    if (buffer.length > 0) {
        writeStream.write(buffer.join("\n") + "\n");
        console.log(`💾 Flushed ${buffer.length} records to disk`);
        buffer = [];
    }
}, FLUSH_INTERVAL_MS);

function saveRecord(record: PriceRecord) {
    const line = JSON.stringify(record);
    buffer.push(line);

    // Flush se buffer ficar grande
    if (buffer.length >= BATCH_SIZE) {
        setImmediate(() => {
            writeStream.write(buffer.join("\n") + "\n");
            console.log(`💾 Flushed ${buffer.length} records (batch full)`);
            buffer = [];
        });
    }
}

// ==================== BINANCE ====================
function setupBinance(): WebSocket {
    const ws = new WebSocket(BINANCE_URL);

    ws.on("open", () => {
        console.log("🟡 Binance: Connected to aggTrade stream");
    });

    ws.on("message", (data: WebSocket.Data) => {
        const tsReceived = Date.now();
        const msg = JSON.parse(data.toString());

        const price = parseFloat(msg.p);
        const qty = parseFloat(msg.q);
        const tsSource = msg.T; // Timestamp da trade (ms)
        const isSell = msg.m;

        const record: PriceRecord = {
            source: "binance",
            price,
            ts_source: tsSource,
            ts_received: tsReceived,
            latency_ms: tsReceived - tsSource,
            extra: {
                qty,
                side: isSell ? "SELL" : "BUY",
            },
        };

        // console.log(
        //     `🟡 Binance: $${price.toFixed(2)} | ` +
        //     `source: ${new Date(tsSource).toISOString()} | ` +
        //     `latency: ${record.latency_ms}ms`
        // );

        saveRecord(record);
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

// ==================== CHAINLINK (Polymarket) ====================
function setupChainlink(): void {
    const onMessage = (_: RealTimeDataClient, message: Message): void => {
        const tsReceived = Date.now();
        const payload = message.payload as Record<string, unknown>;

        if (!payload || payload.value == null) {
            return;
        }

        const price = Number(payload.value);
        const tsSource = payload.timestamp as number;

        // Ignora duplicatas exatas
        if (price === lastChainlinkPrice && tsSource === lastChainlinkTs) {
            return;
        }

        lastChainlinkPrice = price;
        lastChainlinkTs = tsSource;

        // Chainlink timestamp pode estar em segundos, normaliza para ms
        const tsSourceMs = tsSource > 1e12 ? tsSource : tsSource * 1000;

        const record: PriceRecord = {
            source: "chainlink",
            price,
            ts_source: tsSourceMs,
            ts_received: tsReceived,
            latency_ms: tsReceived - tsSourceMs,
        };

        // console.log(
        //     `🔵 Chainlink: $${price.toFixed(2)} | ` +
        //     `source: ${new Date(tsSourceMs).toISOString()} | ` +
        //     `latency: ${record.latency_ms}ms`
        // );

        saveRecord(record);
    };

    const onConnect = (client: RealTimeDataClient): void => {
        console.log("🔵 Chainlink: Connected to Polymarket Real-Time Data");
        client.subscribe({
            subscriptions: [
                {
                    topic: "crypto_prices_chainlink",
                    type: "*",
                    filters: '{"symbol":"btc/usd"}',
                },
            ],
        });
        console.log("🔵 Chainlink: Subscribed to BTC/USD feed");
    };

    new RealTimeDataClient({ onConnect, onMessage }).connect();
}

// ==================== MAIN ====================
async function main(): Promise<void> {
    console.log("🚀 Starting combined BTC price stream...");
    console.log(`📁 Saving to: ${HISTORY_FILE}`);
    console.log("");

    // Inicia ambas as conexões
    setupBinance();
    setupChainlink();

    // Graceful shutdown
    process.on("SIGINT", () => {
        console.log("\n👋 Shutting down...");

        // Flush final
        if (buffer.length > 0) {
            writeStream.write(buffer.join("\n") + "\n");
            console.log(`💾 Final flush: ${buffer.length} records`);
        }

        clearInterval(flushInterval);
        writeStream.end();
        process.exit(0);
    });
}

main();
