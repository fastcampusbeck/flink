package com.fastcampus.streaming.flinkcourse.chapter2.serde;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.java.tuple.Tuple2;

import java.nio.charset.StandardCharsets;

public class TokenCountSerializationSchema implements SerializationSchema<Tuple2<String, Integer>> {
    @Override
    public byte[] serialize(Tuple2<String, Integer> tokenCount) {
        return (tokenCount.f0 + "," + tokenCount.f1 + "\n").getBytes(StandardCharsets.UTF_8);
    }
}