package com.atguigu.shizhan;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Iterator;

public class Test01_Top10 {

    public static void main(String[] args) {

        // 1.创建配置对象
        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("sparkCore");

        // 2. 创建sparkContext
        JavaSparkContext sc = new JavaSparkContext(conf);

        // 3. 编写代码
        JavaRDD<String> javaRDD = sc.textFile("D:\\Spark\\SparkCore\\input\\user_visit_action.txt", 4);

        JavaRDD<UserVisitAction> mapRDD = javaRDD.map(new Function<String, UserVisitAction>() {
            @Override
            public UserVisitAction call(String s) throws Exception {
                String[] s1 = s.split("_");
                UserVisitAction user = new UserVisitAction(s1[0], s1[1], s1[2], s1[3], s1[4], s1[5], s1[6], s1[7], s1[8], s1[9], s1[10], s1[11], s1[12]);

                return user;
            }
        });
        JavaRDD<CategoryCountInfo> flatMapRDD = mapRDD.flatMap(new FlatMapFunction<UserVisitAction, CategoryCountInfo>() {
            @Override
            public Iterator<CategoryCountInfo> call(UserVisitAction userVisitAction) throws Exception {

                ArrayList<CategoryCountInfo> categoryCountInfos = new ArrayList<>();
                if (!userVisitAction.getClick_category_id().equals("-1")) {
                    categoryCountInfos.add(new CategoryCountInfo(userVisitAction.getClick_category_id(), 1L, 0L, 0L));
                } else if (userVisitAction.getOrder_category_ids() != null) {
                    String[] split = userVisitAction.getOrder_category_ids().split(",");
                    for (String s : split) {
                        categoryCountInfos.add(new CategoryCountInfo(s, 0L, 1L, 0L));
                    }
                } else if (userVisitAction.getPay_category_ids() != null) {
                    String[] pays = userVisitAction.getPay_category_ids().split(",");
                    for (String pay : pays) {
                        categoryCountInfos.add(new CategoryCountInfo(pay, 0L, 0L, 1L));
                    }
                }
                return categoryCountInfos.iterator();
            }
        });

        JavaPairRDD<String, CategoryCountInfo> infoJavaPairRDD = flatMapRDD.mapToPair(new PairFunction<CategoryCountInfo, String, CategoryCountInfo>() {
            @Override
            public Tuple2<String, CategoryCountInfo> call(CategoryCountInfo categoryCountInfo) throws Exception {

                return new Tuple2<>(categoryCountInfo.getCategoryId(), categoryCountInfo);
            }
        });
        JavaPairRDD<String, CategoryCountInfo> reduceRDD = infoJavaPairRDD.reduceByKey(new Function2<CategoryCountInfo, CategoryCountInfo, CategoryCountInfo>() {
            @Override
            public CategoryCountInfo call(CategoryCountInfo c1, CategoryCountInfo c2) throws Exception {
                c1.setClickCount(c1.getClickCount() + c2.getClickCount());
                c1.setOrderCount(c1.getOrderCount() + c2.getOrderCount());
                c1.setPayCount(c1.getPayCount() + c2.getPayCount());
                return c1;
            }
        });
        JavaRDD<CategoryCountInfo> map = reduceRDD.map(Tuple2::_2);
        JavaRDD<CategoryCountInfo> sortRDD = map.sortBy(new Function<CategoryCountInfo, CategoryCountInfo>() {
            @Override
            public CategoryCountInfo call(CategoryCountInfo categoryCountInfo) throws Exception {
                return categoryCountInfo;
            }
        }, false, 2);
        sortRDD.take(10).forEach(System.out::println);
        // 4. 关闭sc
        sc.stop();

    }
}
