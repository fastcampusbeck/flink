package com.fastcampus.streaming.flinkcourse.chapter6;

import org.apache.flink.ml.clustering.kmeans.KMeans;
import org.apache.flink.ml.clustering.kmeans.KMeansModel;
import org.apache.flink.ml.linalg.DenseVector;
import org.apache.flink.ml.linalg.Vectors;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;

public class KMeansExample {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        // Generates input data.
        DataStream<DenseVector> inputStream =
                env.fromElements(
                        Vectors.dense(0.0, 0.0),
                        Vectors.dense(0.0, 0.3),
                        Vectors.dense(0.3, 0.0),
                        Vectors.dense(9.0, 0.0),
                        Vectors.dense(9.0, 0.6),
                        Vectors.dense(9.6, 0.0));
        Table inputTable = tEnv.fromDataStream(inputStream).as("features");

        // Creates a K-means object and initializes its parameters.
        KMeans kmeans = new KMeans().setK(2).setSeed(1L);

        // Trains the K-means Model.
        KMeansModel kmeansModel = kmeans.fit(inputTable);

        // Uses the K-means Model for predictions.
        Table outputTable = kmeansModel.transform(inputTable)[0];

        // Extracts and displays the results.
        for (CloseableIterator<Row> it = outputTable.execute().collect(); it.hasNext(); ) {
            Row row = it.next();
            DenseVector features = (DenseVector) row.getField(kmeans.getFeaturesCol());
            int clusterId = (Integer) row.getField(kmeans.getPredictionCol());
            System.out.printf("Features: %s \tCluster ID: %s\n", features, clusterId);
        }
    }
}
