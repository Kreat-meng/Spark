package com.atguigu.creater;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;

import java.util.Arrays;
import java.util.Iterator;

public class FileRDD {
    public static void main(String[] args) {

        //创建配置sparkconf
        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("sparkcore");

        //创建sc

        JavaSparkContext sc = new JavaSparkContext(conf);
        //编写rdd
        JavaRDD<String> fileRDD = sc.textFile("D:\\Spark\\SparkCore\\input\\1.txt", 2);

        JavaRDD<String> mapRDD = fileRDD.map(new Function<String, String>() {
            @Override
            public String call(String line) throws Exception {
                return line + "||";
            }
        });
        JavaRDD<String> flatMapRDD = mapRDD.flatMap(s -> Arrays.asList(s.split(" ")).iterator());
        JavaPairRDD<String, Iterable<String>> groupByRDD = flatMapRDD.groupBy(new Function<String, String>() {
            @Override
            public String call(String s) throws Exception {
                return "a";
            }
        });
        

        //关闭sc
        sc.stop();
    }
}
