package com.fastcampus.streaming.flinkcourse.chapter4.mapstate.database;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

public class DatabaseInitializer {
    public static void main(String[] args) throws Exception {
        Connection connection = DriverManager.getConnection("jdbc:h2:~/mapstate", "beck", "");

        try (Statement statement = connection.createStatement()) {
            statement.execute("CREATE TABLE IF NOT EXISTS AssetRisk (asset_type VARCHAR(255), risk_weight DOUBLE)");
        }

        try (Statement statement = connection.createStatement()) {
            statement.execute("INSERT INTO AssetRisk (asset_type, risk_weight) VALUES ('Stocks', 0.7)");
            statement.execute("INSERT INTO AssetRisk (asset_type, risk_weight) VALUES ('Bonds', 0.3)");
            statement.execute("INSERT INTO AssetRisk (asset_type, risk_weight) VALUES ('Commodities', 0.6)");
            statement.execute("INSERT INTO AssetRisk (asset_type, risk_weight) VALUES ('Real Estate', 0.5)");
            statement.execute("INSERT INTO AssetRisk (asset_type, risk_weight) VALUES ('Cryptocurrency', 1.0)");
        }

        connection.close();
    }
}
