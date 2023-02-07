package com.atguigu.creater;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class PairRDD {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("sparkcore");

        JavaSparkContext sc = new JavaSparkContext(conf);

        List<Tuple2<Integer, String>> arrs= Arrays.asList(new Tuple2<Integer, String>(1, "Holle"), new Tuple2<Integer, String>(2, "spark"),
                new Tuple2<Integer, String>(3, "bababa"));
        JavaPairRDD<Integer, String> pairRDD = sc.parallelizePairs(arrs,3);

        pairRDD.collect().forEach(System.out::println);


        sc.stop();
    }
}
