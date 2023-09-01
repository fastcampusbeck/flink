package com.fastcampus.streaming.flinkcourse.semi.operator.connect;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.ml.linalg.DenseVector;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static com.fastcampus.streaming.flinkcourse.semi.util.TimeConvertor.convertTimestampToString;

public class RepresentativeNewsFunction extends CoProcessFunction<Tuple4<DenseVector, String, Long, Integer>, Tuple5<DenseVector, String, Long, Integer, Integer>, String> {
    private static final int MAX_NEWS_DATA_STATE_SIZE = 1000;

    private transient MapState<Integer, DenseVector> avgVectorState;
    private transient ListState<Tuple2<DenseVector, String>> newsDataState;

    @Override
    public void open(Configuration parameters) {
        MapStateDescriptor<Integer, DenseVector> avgVectorStateDescriptor = new MapStateDescriptor<>("avgVectorState", Integer.class, DenseVector.class);
        avgVectorState = getRuntimeContext().getMapState(avgVectorStateDescriptor);

        ListStateDescriptor<Tuple2<DenseVector, String>> newsDataStateDescriptor = new ListStateDescriptor<>("newsDataState", TypeInformation.of(new TypeHint<>() {}));
        newsDataState = getRuntimeContext().getListState(newsDataStateDescriptor);
    }

    @Override
    public void processElement1(Tuple4<DenseVector, String, Long, Integer> value, Context ctx, Collector<String> out) throws Exception {
        newsDataState.add(new Tuple2<>(value.f0, value.f1));

        Iterator<Tuple2<DenseVector, String>> iterator = newsDataState.get().iterator();
        List<Tuple2<DenseVector, String>> allNewsData = new ArrayList<>();
        while (iterator.hasNext()) {
            allNewsData.add(iterator.next());
        }

        while (allNewsData.size() > MAX_NEWS_DATA_STATE_SIZE) {
            allNewsData.remove(0);
        }

        newsDataState.clear();
        for (Tuple2<DenseVector, String> newsData : allNewsData) {
            newsDataState.add(newsData);
        }

        out.collect("Cluster Number: " + value.f3 + ", Current News: " + value.f1 +
                ", Broadcast Date: " + convertTimestampToString(value.f2));
    }

    @Override
    public void processElement2(Tuple5<DenseVector, String, Long, Integer, Integer> avgVectorValue, Context ctx, Collector<String> out) throws Exception {
        avgVectorState.put(avgVectorValue.f3, avgVectorValue.f0);
        DenseVector avgVector = avgVectorState.get(avgVectorValue.f3);

        double minDistance = Double.MAX_VALUE;
        String closestNews = "";

        for (Tuple2<DenseVector, String> newsData : newsDataState.get()) {
            double currDistance = calculateEuclideanDistance(newsData.f0, avgVector);
            if (currDistance < minDistance) {
                minDistance = currDistance;
                closestNews = newsData.f1;
            }
        }

        if (!closestNews.isEmpty()) {
            out.collect("Cluster Number: " + avgVectorValue.f3 + ", Closest News: " + closestNews +
                    ", Latest Broadcast Date: " + convertTimestampToString(avgVectorValue.f2));
        }
    }

    private static double calculateEuclideanDistance(DenseVector a, DenseVector b) {
        double sum = 0.0;
        for (int i = 0; i < a.size(); i++) {
            sum += Math.pow(a.get(i) - b.get(i), 2);
        }
        return Math.sqrt(sum);
    }
}