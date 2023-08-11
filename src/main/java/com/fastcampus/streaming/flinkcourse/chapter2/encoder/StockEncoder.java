package com.fastcampus.streaming.flinkcourse.chapter2.encoder;

import com.fastcampus.streaming.flinkcourse.model.stock.Stock;
import org.apache.flink.api.common.serialization.Encoder;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;

public class StockEncoder implements Encoder<Stock> {
    @Override
    public void encode(Stock stock, OutputStream outputStream) throws IOException {
        String csvLine = stock.getSymbol() + "," + stock.getPrice() + "," + stock.getTimestamp() + "\n";
        outputStream.write(csvLine.getBytes(StandardCharsets.UTF_8));
    }
}
