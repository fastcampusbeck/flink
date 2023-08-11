package com.fastcampus.streaming.flinkcourse.chapter2;

import com.fastcampus.streaming.flinkcourse.chapter2.encoder.StockEncoder;
import com.fastcampus.streaming.flinkcourse.model.stock.Stock;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.csv.CsvReaderFormat;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;

import java.io.File;
import java.time.Duration;

public class FileSourceAndSink {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        String csvFilePath = "src/main/resources/sample/stock.csv";
        CsvReaderFormat<Stock> csvFormat = CsvReaderFormat.forPojo(Stock.class);
        final FileSource<Stock> source = FileSource
                .forRecordStreamFormat(csvFormat, Path.fromLocalFile(new File(csvFilePath)))
                .build();

        DataStream<Stock> stocks = env.fromSource(
                source, WatermarkStrategy.noWatermarks(), "stock-csv-file-source");

        stocks.print();

        DataStream<String> symbols = stocks.map(Stock::getSymbol);

        symbols.print();

        String outputFilePath = "src/main/resources/sample/output/file";

        final FileSink<String> symbolSink = FileSink
                .forRowFormat(new Path(outputFilePath), new SimpleStringEncoder<String>("UTF-8"))
                .withRollingPolicy(
                        DefaultRollingPolicy.builder()
                                .withRolloverInterval(Duration.ofMinutes(1))
                                .withInactivityInterval(Duration.ofSeconds(10))
                                .withMaxPartSize(MemorySize.ofMebiBytes(1))
                                .build()
                ).build();
        symbols.sinkTo(symbolSink);

        StockEncoder encoder = new StockEncoder();
        final FileSink<Stock> stockSink = FileSink
                .forRowFormat(new Path(outputFilePath), encoder)
                .withRollingPolicy(
                        DefaultRollingPolicy.builder()
                                .withRolloverInterval(Duration.ofMinutes(1))
                                .withInactivityInterval(Duration.ofSeconds(10))
                                .withMaxPartSize(MemorySize.ofMebiBytes(1))
                                .build()
                ).build();
        stocks.sinkTo(stockSink);

        env.execute("File Source and Sink example");
    }

}
