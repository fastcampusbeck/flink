package com.fastcampus.streaming.flinkcourse.chapter3.connect.comap.function;

import com.fastcampus.streaming.flinkcourse.model.prediction.RiskScore;
import com.fastcampus.streaming.flinkcourse.model.stock.StockTransaction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.dmg.pmml.PMML;
import org.jpmml.evaluator.*;

import java.io.InputStream;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

public class RiskScoreRichMapFunction extends RichMapFunction<StockTransaction, RiskScore> {
    private Evaluator evaluator;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        try (InputStream modelStream = getClass().getResourceAsStream("/pmml/riskScoreModelRandomForest.pmml")) {
            PMML pmml = org.jpmml.model.PMMLUtil.unmarshal(modelStream);
            evaluator = new ModelEvaluatorBuilder(pmml).build();
            evaluator.verify();
        }
    }

    @Override
    public RiskScore map(StockTransaction stockTransaction) throws Exception {
        Map<String, Object> features = new LinkedHashMap<>();
        for (InputField inputField : evaluator.getInputFields()) {
            String inputFieldName = inputField.getName();

            Object rawFieldValue = stockTransaction.getVolume();
            FieldValue inputFieldValue = inputField.prepare(rawFieldValue);
            features.put(inputFieldName, inputFieldValue);
        }

        // Apply the model
        Map<String, ?> results = evaluator.evaluate(features);
        TargetField targetField = evaluator.getTargetFields().get(0);

        String targetFieldName = targetField.getName();
        Object targetFieldValue = results.get(targetFieldName);

        String targetFieldValueStr = targetFieldValue.toString();
        System.out.println(targetFieldValueStr);

        String resultStr = targetFieldValueStr.substring(targetFieldValueStr.indexOf('=') + 1, targetFieldValueStr.indexOf('}'));
        double riskScore = Double.parseDouble(resultStr);

        return new RiskScore(stockTransaction.getSymbol(), stockTransaction.getTimestamp(),
                Optional.of(riskScore).orElse(0.5));
    }
}
