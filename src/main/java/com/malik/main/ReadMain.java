package com.malik.main;

import com.malik.config.ElasticConfig;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;

public class ReadMain {
    public static void main(String[] args) {

        // Instantiate config
        ElasticConfig esConfig = new ElasticConfig(args[0]);

        // Instantiate Spark
        SparkSession sparkSession = SparkSession.builder()
                .appName("ReadMain")
                .config("es.nodes", esConfig.ip())
                .config("es.port", esConfig.port())
                .getOrCreate();
        JavaSparkContext sparkContext = new JavaSparkContext(sparkSession.sparkContext());

        // Load data from Elasticsearch then print the value
        JavaEsSpark.esJsonRDD(sparkContext, esConfig.index() + "/" + esConfig.type(), args[1])
                .foreach(tuple2 -> System.out.println(tuple2._2));
    }
}
