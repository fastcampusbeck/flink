package com.fastcampus.streaming.flinkcourse.kafka.generator.stock;

import com.fastcampus.streaming.flinkcourse.model.stock.StockTransaction;
import org.apache.kafka.clients.producer.KafkaProducer;

public class StockTransactionDataGenerator extends StockDataGenerator {
    private final String exchange;

    public StockTransactionDataGenerator(KafkaProducer<String, String> producer) {
        super(producer);
        this.exchange = null;
    }

    public StockTransactionDataGenerator(KafkaProducer<String, String> producer, String exchange) {
        super(producer);
        this.exchange = exchange;
    }

    @Override
    public void generateAndSend() throws Exception {
        for (int i = 0; i < 100; i++) {
            generateAndSendTransactionData();
            Thread.sleep(100);
        }
    }

    private void generateAndSendTransactionData() throws Exception {
        StockTransaction stockTransaction = createStock();
        String transactionJson = objectMapper.writeValueAsString(stockTransaction);
        String exchangePrefix = exchange == null ? "" : exchange + "-";

        sendRecord(exchangePrefix + "stock-transactions", transactionJson);
    }

    @Override
    protected StockTransaction createStock() {
        String symbol = SYMBOLS.get(random.nextInt(SYMBOLS.size()));
        double price = 100 + random.nextFloat() * 100;
        long timestamp = System.currentTimeMillis() - random.nextInt(1000);
        long volume = random.nextInt(1000); // Generate a random volume

        return new StockTransaction(symbol, price, timestamp, volume);
    }
}
