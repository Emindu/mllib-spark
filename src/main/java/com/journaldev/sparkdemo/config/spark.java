package com.journaldev.sparkdemo.config;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SparkSession;
//import org.springframework.context.annotation.Bean;
//import org.springframework.context.annotation.Configuration;
//
//@Configuration

public class spark {

//    @Bean
    public SparkSession sparkSession(){
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

//        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
        SparkContext sparkContext = new SparkContext(sparkConf);

        SparkSession sparkSession = new SparkSession(sparkContext);
        return sparkSession;
    }
}
