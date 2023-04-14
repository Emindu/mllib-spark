package com.journaldev.sparkdemo;

import com.journaldev.sparkdemo.service.MLService;
import com.journaldev.sparkdemo.service.lyrics.GenrePrediction;
import com.journaldev.sparkdemo.service.lyrics.pipeline.LogisticRegressionPipeline;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.*;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.Reader;
import java.net.InetSocketAddress;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
//import org.springframework.boot.SpringApplication;
//import org.springframework.boot.autoconfigure.SpringBootApplication;
//import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
//import org.springframework.boot.autoconfigure.security.servlet.UserDetailsServiceAutoConfiguration;
import scala.Tuple2;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
//@SpringBootApplication
public class WordCounter {

    static SparkSession  sparkSession;
    static LogisticRegressionPipeline logisticRegressionPipeline;



    private static void setup() {
//        SpringApplication.run(WordCounter.class, args);

        SparkConf sparkConf = new SparkConf()
                .setMaster("local").
                setAppName("JD Word Counter").
                setJars(new String[]{"C:\\Users\\emindup\\Documents\\JD-Spark-WordCount\\spark-distributed-library-1.0-SNAPSHOT.jar"})
                .set("spark.cores.max", "4")
                .set("spark.driver.memory", "2g")
                .set("spark.executor.memory", "4g")
                .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .set("spark.kryoserializer.buffer.max", "128m")
                .set("spark.kryo.registrationRequired", "false")
                .set("spark.sql.shuffle.partitions", "5")
                .set("spark.default.parallelism", "4");

        SparkContext sparkContext = new SparkContext(sparkConf);

        sparkSession = new SparkSession(sparkContext);


        MLService mlService = new MLService();

        logisticRegressionPipeline = new LogisticRegressionPipeline(sparkSession, mlService);

        /**
         * comment if what training
         */


    }

    static GenrePrediction predict(String unknownSong){
        return logisticRegressionPipeline.predict(unknownSong);

    }


    /**
     * for training
     */
    static void train(){
        logisticRegressionPipeline.classify();
    }

    public static void main(String[] args) throws IOException {


        setup();

        //uncomment for training
//        train();
        HttpServer server = HttpServer.create(new InetSocketAddress(8000), 0);
        server.createContext("/test", new MyHandler());
        server.setExecutor(null); // creates a default executor
        server.start();
//        wordCount();
    }

    static class MyHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange t) throws IOException {
            InputStream requestBody = t.getRequestBody();

            StringBuilder textBuilder = new StringBuilder();
            try (Reader reader = new BufferedReader(new InputStreamReader
                    (requestBody, Charset.forName(StandardCharsets.UTF_8.name())))) {
                int c = 0;
                while ((c = reader.read()) != -1) {
                    textBuilder.append((char) c);
                }
            }
            System.out.println(textBuilder.toString());
            GenrePrediction predict = predict(textBuilder.toString());
            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.append("{")
                    .append("Prediction:")
                    .append(predict.getGenre())
                    .append(",")
                    .append("Probability:")
                    .append(predict.getProbability())
                    .append("}");

            String response = stringBuilder.toString();
            t.sendResponseHeaders(200, response.length());
            OutputStream os = t.getResponseBody();
            os.write(response.getBytes());
            os.close();
        }
    }

    private static StructType getTrainingSetSchema() {
        return new StructType(new StructField[] {
                new StructField("value", DataTypes.StringType, true, Metadata.empty()),
                new StructField("label", DataTypes.DoubleType, true, Metadata.empty()),

        });
    }
}
