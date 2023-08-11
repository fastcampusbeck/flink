package com.fastcampus.streaming.flinkcourse.kafka.generator.stock;

import com.fastcampus.streaming.flinkcourse.model.stock.Stock;
import org.apache.kafka.clients.producer.KafkaProducer;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class StockGroupDataGenerator extends StockDataGenerator {
    private List<String> symbols;

    public StockGroupDataGenerator(KafkaProducer<String, String> producer) {
        super(producer);
        this.symbols = new ArrayList<>(SYMBOLS);
    }

    @Override
    public void generateAndSend() throws Exception {
        for (int i = 0; i < 20; i++) {
            generateAndSendStockGroupData();
            Thread.sleep(100);
        }
    }

    private void generateAndSendStockGroupData() throws Exception {
        List<Stock> stockGroup = IntStream.range(0, 5)
                .mapToObj(i -> createStock())
                .collect(Collectors.toList());

        String stockGroupJson = objectMapper.writeValueAsString(stockGroup);

        sendRecord("stock-groups", stockGroupJson);
    }

    @Override
    protected Stock createStock() {
        String symbol = pickRandomSymbol();
        double price = 100 + random.nextFloat() * 100;
        long timestamp = System.currentTimeMillis() - random.nextInt(1000);

        return new Stock(symbol, price, timestamp);
    }

    private String pickRandomSymbol() {
        if (symbols.isEmpty()) {
            symbols = new ArrayList<>(SYMBOLS);
            Collections.shuffle(symbols, random);
        }

        return symbols.remove(symbols.size() - 1);
    }
}

