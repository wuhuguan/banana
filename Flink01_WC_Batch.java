package com.atguigu.flink.chapter02;


import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.*;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;


public class Flink01_WC_Batch {
    public static void main(String[] args) throws Exception {
        // 1、创建执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // 2、读取数据
        DataSource<String> inputDS = env.readTextFile("input/word.txt");
        // 绝对路径：E:\workspace\flink_0820\input\word.txt
        // 3、处理数据
        // 3.1 压平：切分成 word    flatmap 一进多出   map 一进一出
        FlatMapOperator<String, String> wordDS = inputDS.flatMap(
                new FlatMapFunction<String, String>() {
                    @Override
                    public void flatMap(String value, Collector<String> out) throws Exception {
                        // 切分数据
                        String[] words = value.split(" ");
                        for (String word : words) {
                            // 通过采集器，将 word 发往下游
                            out.collect(word);
                        }
                    }
                });
        // 3.2 转换成元组（word，1）
        MapOperator<String, Tuple2<String, Long>> wordAndOneDS = wordDS.map(
                new MapFunction<String, Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> map(String value) throws Exception {
                        return Tuple2.of(value, 1L);
                    }
                });
        // 3.3 按照word进行分组
        UnsortedGrouping<Tuple2<String, Long>> wordAndOneGroup = wordAndOneDS.groupBy(0);
        // 3.4 组内聚合
        AggregateOperator<Tuple2<String, Long>> resultDS = wordAndOneGroup.sum(1);
        // 4、输出
        resultDS.print();
        // 5、执行(flink批处理不需要)

    }
}
