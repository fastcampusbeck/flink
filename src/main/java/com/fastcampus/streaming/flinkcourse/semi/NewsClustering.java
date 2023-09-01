package com.fastcampus.streaming.flinkcourse.semi;

import com.fastcampus.streaming.flinkcourse.semi.model.News;
import com.fastcampus.streaming.flinkcourse.semi.operator.connect.RepresentativeNewsFunction;
import com.fastcampus.streaming.flinkcourse.semi.operator.flatmap.FlattenStoredNewsFunction;
import com.fastcampus.streaming.flinkcourse.semi.operator.flatmap.NewsDataStateStoringFunction;
import com.fastcampus.streaming.flinkcourse.semi.operator.map.ContentToVectorConverter;
import com.fastcampus.streaming.flinkcourse.semi.operator.map.RowToTuple4Function;
import com.fastcampus.streaming.flinkcourse.semi.operator.map.Tuple4ToTuple5Function;
import com.fastcampus.streaming.flinkcourse.semi.operator.reduce.CombineVectorsFunction;
import com.fastcampus.streaming.flinkcourse.semi.source.NewsApiSource;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.ml.clustering.kmeans.KMeansModelData;
import org.apache.flink.ml.clustering.kmeans.OnlineKMeans;
import org.apache.flink.ml.clustering.kmeans.OnlineKMeansModel;
import org.apache.flink.ml.linalg.DenseVector;
import org.apache.flink.ml.linalg.typeinfo.DenseVectorTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Properties;
import java.util.Random;


public class NewsClustering {
    private static final String API_KEY = loadApiKey();
    private static final int CLUSTER_COUNT = 10;
    private static final int RANDOM_MODEL_DATA_COUNT = 100;
    private static final double RANDOM_MODEL_DATA_VALUE = new Random().nextDouble() * 10;

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        DataStream<News> apiDataStream = getApiDataStream(env);

        DataStream<Row> vectorizedContents = apiDataStream.map(new ContentToVectorConverter())
                .returns(new RowTypeInfo(DenseVectorTypeInfo.INSTANCE, Types.STRING, Types.LONG));

        DataStream<List<Row>> storedNewsDataStream = vectorizedContents.keyBy(value -> 1)
                .flatMap(new NewsDataStateStoringFunction());

        DataStream<Row> flattenedStoredNewsDataStream = storedNewsDataStream.flatMap(new FlattenStoredNewsFunction())
                .returns(new RowTypeInfo(DenseVectorTypeInfo.INSTANCE, Types.STRING, Types.LONG));

        Table storedDataTable = tEnv.fromDataStream(flattenedStoredNewsDataStream)
                .as("features", "title", "broadcast_date");

        OnlineKMeans onlineKMeans = getOnlineKMeans(tEnv);
        OnlineKMeansModel model = onlineKMeans.fit(storedDataTable);

        Table predictedTable = model.transform(storedDataTable)[0];
        DataStream<Row> predictedStream = tEnv.toDataStream(predictedTable);

        DataStream<Tuple4<DenseVector, String, Long, Integer>> tuple4Stream = predictedStream.map(new RowToTuple4Function());

        DataStream<Tuple5<DenseVector, String, Long, Integer, Integer>> avgVectorStream = tuple4Stream
                .map(new Tuple4ToTuple5Function())
                .keyBy(tuple -> tuple.f3)
                .reduce(new CombineVectorsFunction());

        DataStream<String> representativeNewsStream = tuple4Stream
                .keyBy(tuple -> tuple.f3)
                .connect(avgVectorStream.keyBy(tuple -> tuple.f3))
                .process(new RepresentativeNewsFunction());

        representativeNewsStream.print();

        env.execute("Real-Time News Clustering");
    }

    private static String loadApiKey() {
        Properties properties = new Properties();
        try (InputStream input = NewsClustering.class.getClassLoader().getResourceAsStream("config.properties")) {
            if (input == null) {
                throw new RuntimeException("Unable to find config.properties");
            }
            properties.load(input);
            return properties.getProperty("API_KEY");
        } catch (IOException ex) {
            throw new RuntimeException("Failed to load API_KEY from config.properties", ex);
        }
    }

    private static DataStream<News> getApiDataStream(StreamExecutionEnvironment env) {
        return env.addSource(new NewsApiSource(API_KEY, 10));
    }

    private static OnlineKMeans getOnlineKMeans(StreamTableEnvironment tEnv) {
        return new OnlineKMeans()
                .setFeaturesCol("features")
                .setPredictionCol("prediction")
                .setInitialModelData(KMeansModelData.generateRandomModelData(tEnv, CLUSTER_COUNT, RANDOM_MODEL_DATA_COUNT, RANDOM_MODEL_DATA_VALUE, 0))
                .setK(CLUSTER_COUNT);
    }
}
