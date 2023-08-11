package com.fastcampus.streaming.flinkcourse.chapter2.encoder;

import org.apache.flink.api.common.serialization.Encoder;
import org.apache.flink.api.java.tuple.Tuple2;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;

public class TokenCountEncoder implements Encoder<Tuple2<String, Integer>> {
    @Override
    public void encode(Tuple2<String, Integer> stringIntegerTuple2, OutputStream outputStream) throws IOException {
        String tokenCount = stringIntegerTuple2.f0 + "," + stringIntegerTuple2.f1 + "\n";
        outputStream.write(tokenCount.getBytes(StandardCharsets.UTF_8));
    }
}