package com.fastcampus.streaming.flinkcourse.chapter2;

import com.fastcampus.streaming.flinkcourse.chapter2.encoder.TokenCountEncoder;
import com.fastcampus.streaming.flinkcourse.chapter2.function.Tokenizer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;

import java.time.Duration;

public class SocketSourceAndFileSink {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> text = env.socketTextStream("localhost", 9999);

        DataStream<Tuple2<String, Integer>> tokenCounts =
                text.flatMap(new Tokenizer())
                        .keyBy(tuple -> tuple.f0)
                        .sum(1);
        tokenCounts.print();

        String outputSocketPath = "src/main/resources/sample/output/socket";

        final FileSink<Tuple2<String, Integer>> tokenCountsSink = FileSink
                .forRowFormat(new Path(outputSocketPath), new TokenCountEncoder())
                .withRollingPolicy(
                        DefaultRollingPolicy.builder()
                                .withRolloverInterval(Duration.ofMinutes(1))
                                .withInactivityInterval(Duration.ofSeconds(10))
                                .withMaxPartSize(MemorySize.ofMebiBytes(1))
                                .build()
                ).build();

        tokenCounts.sinkTo(tokenCountsSink);

        env.execute("Socket Source and File Sink example");
    }
}
