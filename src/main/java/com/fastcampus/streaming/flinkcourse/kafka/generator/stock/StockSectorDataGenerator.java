package com.fastcampus.streaming.flinkcourse.kafka.generator.stock;

import com.fastcampus.streaming.flinkcourse.model.stock.Stock;
import org.apache.kafka.clients.producer.KafkaProducer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class StockSectorDataGenerator extends StockDataGenerator {
    private final Map<String, String[]> sectorSymbols;

    public StockSectorDataGenerator(KafkaProducer<String, String> producer) {
        super(producer);
        this.sectorSymbols = new HashMap<>();
        sectorSymbols.put("Technology", new String[] {"AAPL", "MSFT", "GOOGL"});
        sectorSymbols.put("Consumer Discretionary", new String[] {"AMZN"});
        sectorSymbols.put("Communication Services", new String[] {"FB"});
    }

    public void generateAndSend() throws Exception {
        for (int i = 0; i < 20; i++) {
            for (String sector : sectorSymbols.keySet()) {
                generateAndSendSectorData(sector);
                Thread.sleep(100);
            }
        }
    }

    private void generateAndSendSectorData(String sector) throws Exception {
        List<String> sectorData = new ArrayList<>();
        String[] symbols = sectorSymbols.get(sector);

        for (String symbol : symbols) {
            double price = 100 + random.nextFloat() * 100;
            long timestamp = System.currentTimeMillis() - random.nextInt(1000);

            sectorData.add(symbol + ":" + price + ":" + timestamp);
        }

        String sectorDataString = sector + "|" + String.join(",", sectorData);

        sendRecord("stock-sectors", sectorDataString);
    }

    @Override
    protected Stock createStock() {
        return null;
    }
}
