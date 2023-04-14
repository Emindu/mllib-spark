package com.journaldev.sparkdemo.service.lyrics.pipeline;

import com.journaldev.sparkdemo.service.MLService;
import com.journaldev.sparkdemo.service.lyrics.transformer.Cleanser;
import com.journaldev.sparkdemo.service.lyrics.transformer.Exploder;
import com.journaldev.sparkdemo.service.lyrics.transformer.Numerator;
import com.journaldev.sparkdemo.service.lyrics.transformer.Stemmer;
import com.journaldev.sparkdemo.service.lyrics.transformer.Uniter;
import com.journaldev.sparkdemo.service.lyrics.transformer.Verser;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.Transformer;
import org.apache.spark.ml.classification.LogisticRegression;
import org.apache.spark.ml.classification.LogisticRegressionModel;
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator;
import org.apache.spark.ml.feature.StopWordsRemover;
import org.apache.spark.ml.feature.Tokenizer;
import org.apache.spark.ml.feature.Word2Vec;
import org.apache.spark.ml.feature.Word2VecModel;
import org.apache.spark.ml.param.ParamMap;
import org.apache.spark.ml.tuning.CrossValidator;
import org.apache.spark.ml.tuning.CrossValidatorModel;
import org.apache.spark.ml.tuning.ParamGridBuilder;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
//import org.springframework.stereotype.Component;
//import org.springframework.stereotype.Component;

import java.util.Map;

import static com.lohika.morning.ml.spark.distributed.library.function.map.lyrics.Column.*;

//@Component("LogisticRegressionPipeline")
public class LogisticRegressionPipeline extends CommonLyricsPipeline {
    public LogisticRegressionPipeline(SparkSession sparkSession, MLService mlService) {
        this.sparkSession = sparkSession;
        this.mlService = mlService;
    }

    public CrossValidatorModel classify() {
        Dataset<Row> sentences = readLyrics();

        Cleanser cleanser = new Cleanser();

        Numerator numerator = new Numerator();

        Tokenizer tokenizer = new Tokenizer()
                .setInputCol(CLEAN.getName())
                .setOutputCol(WORDS.getName());

        StopWordsRemover stopWordsRemover = new StopWordsRemover()
                .setInputCol(WORDS.getName())
                .setOutputCol(FILTERED_WORDS.getName());

        Exploder exploder = new Exploder();

        Stemmer stemmer = new Stemmer();

        Uniter uniter = new Uniter();
        Verser verser = new Verser();

        Word2Vec word2Vec = new Word2Vec()
                                    .setInputCol(VERSE.getName())
                                    .setOutputCol("features")
                                    .setMinCount(0);

        LogisticRegression logisticRegression = new LogisticRegression();

        Pipeline pipeline = new Pipeline().setStages(
                new PipelineStage[]{
                        cleanser,
                        numerator,
                        tokenizer,
                        stopWordsRemover,
                        exploder,
                        stemmer,
                        uniter,
                        verser,
                        word2Vec,
                        logisticRegression});

        ParamMap[] paramGrid = new ParamGridBuilder()
                .addGrid(verser.sentencesInVerse(), new int[]{4})
                .addGrid(word2Vec.vectorSize(), new int[] {100})
                .addGrid(logisticRegression.regParam(), new double[] {0.01D})
                .addGrid(logisticRegression.maxIter(), new int[] {1})
                .build();

        CrossValidator crossValidator = new CrossValidator()
                .setEstimator(pipeline)
                .setEvaluator(new BinaryClassificationEvaluator())
                .setEstimatorParamMaps(paramGrid)
                .setNumFolds(10);

        CrossValidatorModel model = crossValidator.fit(sentences);

        saveModel(model, getModelDirectory());

        return model;
    }

    public Map<String, Object> getModelStatistics(CrossValidatorModel model) {
        Map<String, Object> modelStatistics = super.getModelStatistics(model);

        PipelineModel bestModel = (PipelineModel) model.bestModel();
        Transformer[] stages = bestModel.stages();

        modelStatistics.put("Sentences in verse", ((Verser) stages[7]).getSentencesInVerse());
        modelStatistics.put("Word2Vec vocabulary", ((Word2VecModel) stages[8]).getVectors().count());
        modelStatistics.put("Vector size", ((Word2VecModel) stages[8]).getVectorSize());
        modelStatistics.put("Reg parameter", ((LogisticRegressionModel) stages[9]).getRegParam());
        modelStatistics.put("Max iterations", ((LogisticRegressionModel) stages[9]).getMaxIter());

        printModelStatistics(modelStatistics);

        return modelStatistics;
    }

    @Override
    protected String getModelDirectory() {
        return getLyricsModelDirectoryPath() + "\\logistic-regression\\";
    }

}
