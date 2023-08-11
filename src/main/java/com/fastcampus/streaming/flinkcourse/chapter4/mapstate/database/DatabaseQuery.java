package com.fastcampus.streaming.flinkcourse.chapter4.mapstate.database;

import com.fastcampus.streaming.flinkcourse.model.asset.AssetTradeWithRisk;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;

import java.sql.*;
import java.util.Collections;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class DatabaseQuery extends RichAsyncFunction<AssetTradeWithRisk, String> {
    private transient ExecutorService executorService;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        executorService = Executors.newFixedThreadPool(100);
    }

    @Override
    public void asyncInvoke(AssetTradeWithRisk assetTradeWithRisk, ResultFuture<String> resultFuture) throws Exception {
        executorService.submit(() -> {
            try (Connection connection = DriverManager.getConnection("jdbc:h2:~/mapstate", "beck", "")) {
                String sql = "SELECT asset_type, risk_weight FROM AssetRisk WHERE asset_type = ?";
                try (PreparedStatement statement = connection.prepareStatement(sql)) {
                    statement.setString(1, assetTradeWithRisk.getAsset());
                    ResultSet resultSet = statement.executeQuery();
                    if (resultSet.next()) {
                        double newRiskWeight = resultSet.getDouble("risk_weight");
                        resultFuture.complete(Collections.singleton("CustomerId: " + assetTradeWithRisk.getCustomerId() +
                                ", AssetType: " + assetTradeWithRisk.getAsset() + ", Amount: " + assetTradeWithRisk.getAmount() +
                                ", IsBuy: " + assetTradeWithRisk.getIsBuy() + ", RiskWeight: " + newRiskWeight));
                    } else {
                        resultFuture.complete(Collections.singleton("No risk weight found for asset type: " +
                                assetTradeWithRisk.getAsset()));
                    }
                }
            } catch (SQLException e) {
                resultFuture.completeExceptionally(e);
            }
        });
    }

    @Override
    public void close() throws Exception {
        super.close();
    }
}
