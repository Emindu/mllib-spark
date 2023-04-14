package com.journaldev.sparkdemo.service.lyrics.pipeline;


import com.journaldev.sparkdemo.service.MLService;
import com.journaldev.sparkdemo.service.lyrics.Genre;
import com.journaldev.sparkdemo.service.lyrics.GenrePrediction;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.linalg.DenseVector;
import org.apache.spark.ml.tuning.CrossValidatorModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
//import org.springframework.beans.factory.annotation.Autowired;

import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static com.lohika.morning.ml.spark.distributed.library.function.map.lyrics.Column.*;

public abstract class CommonLyricsPipeline implements LyricsPipeline {

//    @Autowired
    protected SparkSession sparkSession;

//    @Autowired
    protected MLService mlService;

//    @Value("${lyrics.training.set.directory.path}")
//    private String lyricsTrainingSetDirectoryPath;

//    @Value("${lyrics.model.directory.path}")
    private String lyricsModelDirectoryPath = "D:\\model";

    @Override
    public GenrePrediction predict(final String unknownLyrics) {

        String lyrics[] = unknownLyrics.split("\\r?\\n");
        Dataset<String> lyricsDataset = sparkSession.createDataset(Arrays.asList(lyrics),
           Encoders.STRING());

        Dataset<Row> unknownLyricsDataset = lyricsDataset
                .withColumn(LABEL.getName(), functions.lit(Genre.UNKNOWN.getValue()))
                .withColumn(ID.getName(), functions.lit("unknown.txt"));

        CrossValidatorModel model = mlService.loadCrossValidationModel(getModelDirectory());
        getModelStatistics(model);

        PipelineModel bestModel = (PipelineModel) model.bestModel();

        Dataset<Row> predictionsDataset = bestModel.transform(unknownLyricsDataset);
        Row predictionRow = predictionsDataset.first();

        System.out.println("\n------------------------------------------------");
        final Double prediction = predictionRow.getAs("prediction");
        System.out.println("Prediction: " + Double.toString(prediction));
        GenrePrediction genrePrediction = new GenrePrediction(prediction.toString());

        if (Arrays.asList(predictionsDataset.columns()).contains("probability")) {
            final DenseVector probability = predictionRow.getAs("probability");
            System.out.println("Probability: " + probability);
            genrePrediction.setProbability(probability.toString());
            System.out.println("------------------------------------------------\n");

            return new GenrePrediction(getGenre(prediction).getName(), probability.apply(0), probability.apply(1));
        }

        System.out.println("------------------------------------------------\n");
//        return new GenrePrediction(getGenre(prediction).getName());


        return  genrePrediction;
    }

    Dataset<Row> readLyrics() {
//        Dataset input = readLyricsForGenre(lyricsTrainingSetDirectoryPath, Genre.METAL)
//                                                .union(readLyricsForGenre(lyricsTrainingSetDirectoryPath, Genre.POP));
//        // Reduce the input amount of partition minimal amount (spark.default.parallelism OR 2, whatever is less)
//        input = input.coalesce(sparkSession.sparkContext().defaultMinPartitions()).cache();
//        // Force caching.
//        input.count();
//
//        return input;
        Dataset<Row> rawTrainingSet = sparkSession
                .read()
                .option("header", "true")
                .schema(getTrainingSetSchema())
                .csv("C:\\Users\\emindup\\Documents\\JD-Spark-WordCount\\src\\main\\resources\\cc_ceds_music.csv");

        rawTrainingSet = rawTrainingSet.coalesce(sparkSession.sparkContext().defaultMinPartitions()).cache();
        // Force caching.
        rawTrainingSet.count();
        return rawTrainingSet;
    }

    private Dataset<Row> readLyricsForGenre(String inputDirectory, Genre genre) {
        Dataset<Row> lyrics = readLyrics(inputDirectory, genre.name().toLowerCase() + "/*");
        Dataset<Row> labeledLyrics = lyrics.withColumn(LABEL.getName(), functions.lit(genre.getValue()));

        System.out.println(genre.name() + " music sentences = " + lyrics.count());

        return labeledLyrics;
    }

    private Dataset<Row> readLyrics(String inputDirectory, String path) {
        Dataset<String> rawLyrics = sparkSession.read().textFile(Paths.get("D:\\BDA\\cc_ceds_music.csv").toString());
        rawLyrics = rawLyrics.filter(rawLyrics.col(VALUE.getName()).notEqual(""));
        rawLyrics = rawLyrics.filter(rawLyrics.col(VALUE.getName()).contains(" "));

        // Add source filename column as a unique id.
        Dataset<Row> lyrics = rawLyrics.withColumn(ID.getName(), functions.input_file_name());


        Dataset<Row> rawTrainingSet = sparkSession
                .read()
                .option("header", "true")
                .schema(getTrainingSetSchema())
                .csv("D:\\BDA\\cc_ceds_music.csv");

        rawTrainingSet.count();
        rawTrainingSet.cache();

        return rawTrainingSet;

    }



    private Genre getGenre(Double value) {
        for (Genre genre: Genre.values()){
            if (genre.getValue().equals(value)) {
                return genre;
            }
        }

        return Genre.UNKNOWN;
    }

    @Override
    public Map<String, Object> getModelStatistics(CrossValidatorModel model) {
        Map<String, Object> modelStatistics = new HashMap<>();

        Arrays.sort(model.avgMetrics());
        modelStatistics.put("Best model metrics", model.avgMetrics()[model.avgMetrics().length - 1]);

        return modelStatistics;
    }

    void printModelStatistics(Map<String, Object> modelStatistics) {
        System.out.println("\n------------------------------------------------");
        System.out.println("Model statistics:");
        System.out.println(modelStatistics);
        System.out.println("------------------------------------------------\n");
    }

    void saveModel(CrossValidatorModel model, String modelOutputDirectory) {
        this.mlService.saveModel(model, modelOutputDirectory);
    }

    void saveModel(PipelineModel model, String modelOutputDirectory) {
        this.mlService.saveModel(model, modelOutputDirectory);
    }

//    public void setLyricsTrainingSetDirectoryPath(String lyricsTrainingSetDirectoryPath) {
//        this.lyricsTrainingSetDirectoryPath = lyricsTrainingSetDirectoryPath;
//    }

    public void setLyricsModelDirectoryPath(String lyricsModelDirectoryPath) {
        this.lyricsModelDirectoryPath = lyricsModelDirectoryPath;
    }

    protected abstract String getModelDirectory();

    String getLyricsModelDirectoryPath() {
        return lyricsModelDirectoryPath;
    }

    private StructType getTrainingSetSchema() {
        return new StructType(new StructField[] {
                new StructField("id", DataTypes.StringType, true, Metadata.empty()),
                new StructField("value", DataTypes.StringType, true, Metadata.empty()),
                new StructField("label", DataTypes.DoubleType, true, Metadata.empty()),


        });
    }

}
