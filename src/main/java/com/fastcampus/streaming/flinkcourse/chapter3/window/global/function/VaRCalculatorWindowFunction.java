package com.fastcampus.streaming.flinkcourse.chapter3.window.global.function;

import com.fastcampus.streaming.flinkcourse.model.portfolio.Investment;
import com.fastcampus.streaming.flinkcourse.model.portfolio.Portfolio;
import com.fastcampus.streaming.flinkcourse.model.portfolio.VaR;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;

public class VaRCalculatorWindowFunction implements WindowFunction<Portfolio, VaR, String, GlobalWindow> {
    @Override
    public void apply(String s, GlobalWindow globalWindow, Iterable<Portfolio> iterable, Collector<VaR> collector) throws Exception {
        VaR vaR = calculateVaR(iterable);
        collector.collect(vaR);
    }

    private VaR calculateVaR(Iterable<Portfolio> portfolios) {
        String portfolioId = null;
        double totalValue = 0.0;

        for (Portfolio portfolio : portfolios) {
            for (Investment investment : portfolio.getInvestments()) {
                totalValue += investment.getValue();
            }

            if (portfolioId == null) {
                portfolioId = portfolio.getId();
            }
        }

        double var = totalValue * 0.05; // Assume a 5% risk factor

        return new VaR(portfolioId, var);
    }
}
