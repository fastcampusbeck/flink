package com.fastcampus.streaming.flinkcourse.kafka.generator.portfolio;

import com.fastcampus.streaming.flinkcourse.model.portfolio.Investment;
import com.fastcampus.streaming.flinkcourse.model.portfolio.Portfolio;
import org.apache.kafka.clients.producer.KafkaProducer;

import java.util.ArrayList;
import java.util.List;

public class PortfolioSingleDataGenerator extends PortfolioDataGenerator {
    public PortfolioSingleDataGenerator(KafkaProducer<String, String> producer) {
        super(producer);
    }

    @Override
    public void generateAndSend() throws Exception {
        for (int i = 0; i < 100; i++) {
            generateAndSendPortfolioData();
            Thread.sleep(100);
        }
    }

    private void generateAndSendPortfolioData() throws Exception {
        Portfolio portfolio = createPortfolio();

        String portfolioJson = objectMapper.writeValueAsString(portfolio);

        sendRecord("portfolios", portfolioJson);
    }

    protected Portfolio createPortfolio() {
        String id = "portfolio-" + random.nextInt(1000); // Generate random id
        List<Investment> investments = createInvestments();
        long timestamp = System.currentTimeMillis() - random.nextInt(2 * 60 * 60 * 1000); // 2 hours in milliseconds
        long validityPeriod = 60 * 60 * 1000; // 1 hour in milliseconds

        return new Portfolio(id, investments, timestamp, validityPeriod);
    }

    private List<Investment> createInvestments() {
        List<Investment> investments = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            String investmentId = "investment-" + random.nextInt(1000); // Generate unique id for each investment
            double value = 1000 + random.nextFloat() * 9000; // Random value between 1000 and 10000
            investments.add(new Investment(investmentId, value));
        }
        return investments;
    }
}
