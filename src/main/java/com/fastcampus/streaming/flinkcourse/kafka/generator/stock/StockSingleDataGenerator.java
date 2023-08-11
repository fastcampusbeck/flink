package com.fastcampus.streaming.flinkcourse.kafka.generator.stock;

import com.fastcampus.streaming.flinkcourse.model.stock.Stock;
import org.apache.kafka.clients.producer.KafkaProducer;

import java.time.ZoneId;
import java.time.ZonedDateTime;

public class StockSingleDataGenerator extends StockDataGenerator {
    public StockSingleDataGenerator(KafkaProducer<String, String> producer) {
        super(producer);
    }

    @Override
    public void generateAndSend() throws Exception {
        for (int i = 0; i < 100; i++) {
            generateAndSendStockData();
            Thread.sleep(100);
        }
    }

    private void generateAndSendStockData() throws Exception {
        Stock stock = createStock();

        String stockJson = objectMapper.writeValueAsString(stock);

        sendRecord("stocks", stockJson);
    }

    @Override
    protected Stock createStock() {
        String symbol = SYMBOLS.get(random.nextInt(SYMBOLS.size()));
        double price = 100 + random.nextFloat() * 100;
        ZonedDateTime nowInKst = ZonedDateTime.now(ZoneId.of("Asia/Seoul"));
        long timestamp = System.currentTimeMillis() - random.nextInt(1000);

        return new Stock(symbol, price, timestamp);
    }
}
