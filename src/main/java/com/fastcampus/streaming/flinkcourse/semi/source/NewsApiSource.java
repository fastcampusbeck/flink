package com.fastcampus.streaming.flinkcourse.semi.source;

import com.fastcampus.streaming.flinkcourse.semi.model.News;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;

import static com.fastcampus.streaming.flinkcourse.semi.util.TimeConvertor.convertStringToTimestamp;

public class NewsApiSource extends RichSourceFunction<News> {
    private final String serviceKey;
    private int pageNo;
    private final int numOfRows;
    private volatile boolean isCanceled = false;

    public NewsApiSource(String serviceKey, int numOfRows) {
        this.serviceKey = serviceKey;
        this.numOfRows = numOfRows;
    }

    @Override
    public void run(SourceFunction.SourceContext<News> ctx) throws Exception {
        while (!isCanceled) {

            this.pageNo += 1;

            String response = fetchDataFromApi(serviceKey, pageNo, numOfRows);

            JSONObject jsonResponse;
            try {
                jsonResponse = new JSONObject(response);
            } catch (JSONException e) {
                System.err.println("Failed to parse JSON: " + e.getMessage());
                return;
            }

            if (jsonResponse.has("items")) {
                JSONArray items = jsonResponse.getJSONArray("items");
                for (int i = 0; i < items.length(); i++) {
                    JSONObject item = items.getJSONObject(i);
                    String title = item.getString("title");
                    String content = item.getString("content");
                    long broadcast_date = convertStringToTimestamp(item.getString("broadcast_date"));
                    News news = new News(title, content, broadcast_date);
                    ctx.collect(news);
                }
            } else {
                System.err.println("Invalid API response: No 'items' found.");
            }

        Thread.sleep(1000);
        }
    }


    @Override
    public void cancel() {
        isCanceled = true;
    }

    private String fetchDataFromApi(String serviceKey, int pageNo, int numOfRows) throws IOException {
        String apiUrl = String.format("https://apis.data.go.kr/B551024/openArirangNewsApi/news?serviceKey=%s&pageNo=%d&numOfRows=%d",
                serviceKey, pageNo, numOfRows);
        URL url = new URL(apiUrl);
        HttpURLConnection connection = (HttpURLConnection) url.openConnection();
        connection.setRequestMethod("GET");
        connection.setRequestProperty("Accept", "application/json");

        if (connection.getResponseCode() != 200) {
            throw new RuntimeException("Failed : HTTP error code : " + connection.getResponseCode());
        }

        BufferedReader br = new BufferedReader(new InputStreamReader((connection.getInputStream())));
        String output;
        StringBuilder response = new StringBuilder();

        while ((output = br.readLine()) != null) {
            response.append(output);
        }

        connection.disconnect();
        return response.toString();
    }

}
