package com.fastcampus.streaming.flinkcourse.chapter2;

import com.fastcampus.streaming.flinkcourse.chapter2.function.Tokenizer;
import com.fastcampus.streaming.flinkcourse.chapter2.serde.TokenCountSerializationSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class SocketSourceAndSink {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> text = env.socketTextStream("localhost", 9999);
        text.print();

        DataStream<Tuple2<String, Integer>> tokenCounts =
                text.flatMap(new Tokenizer())
                        .keyBy(tuple -> tuple.f0)
                        .sum(1);

        tokenCounts.print();

        tokenCounts.writeToSocket("localhost", 10000, new TokenCountSerializationSchema());

        env.execute("Socket Source and Sink example");
    }
}
