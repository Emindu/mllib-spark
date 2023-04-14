package com.journaldev.sparkdemo.controller;

import com.journaldev.sparkdemo.service.lyrics.GenrePrediction;
import com.journaldev.sparkdemo.service.lyrics.pipeline.LogisticRegressionPipeline;
import org.apache.spark.ml.tuning.CrossValidatorModel;

import java.util.Map;

//import org.springframework.beans.factory.annotation.Autowired;
//import org.springframework.stereotype.Component;
//
//import javax.annotation.Resource;
//import java.util.Map;
//
//@Component
public class LyricsService {

//    @Autowired
    private LogisticRegressionPipeline pipeline;

    public Map<String, Object> classifyLyrics() {
        CrossValidatorModel model = pipeline.classify();
        return pipeline.getModelStatistics(model);
    }

    public GenrePrediction predictGenre(final String unknownLyrics) {
        return pipeline.predict(unknownLyrics);
    }

}
