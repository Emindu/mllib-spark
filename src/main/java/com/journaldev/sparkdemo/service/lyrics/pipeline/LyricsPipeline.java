package com.journaldev.sparkdemo.service.lyrics.pipeline;

import com.journaldev.sparkdemo.service.lyrics.GenrePrediction;
import org.apache.spark.ml.tuning.CrossValidatorModel;

import java.util.Map;

public interface LyricsPipeline {

    CrossValidatorModel classify();

    GenrePrediction predict(String unknownLyrics);

    Map<String, Object> getModelStatistics(CrossValidatorModel model);

}
