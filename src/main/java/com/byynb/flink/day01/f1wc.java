package com.byynb.flink.day01;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * @auther byy
 * @create 2021-01-17-17:51
 */
public class f1wc {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSource<String> input = env.readTextFile("E:\\project\\byynb\\flink\\input");
        FlatMapOperator<String, Tuple2<String, Integer>> line2TupleDS = input.flatMap(new line2TupleFlatMap());
       /* line2TupleDS.groupBy(new KeySelector<Tuple2<String, Integer>, Object>() {
            @Override
            public Object getKey(Tuple2<String, Integer> value) throws Exception {
                return value.f0;
            }
        });*/
        UnsortedGrouping<Tuple2<String, Integer>> groupByDS = line2TupleDS.groupBy(0);
        AggregateOperator<Tuple2<String, Integer>> result = groupByDS.sum(1);
        result.print();
    }

    // line =>(word,1)
    public static class line2TupleFlatMap implements FlatMapFunction<String,Tuple2<String,Integer>> {

        @Override
        public void flatMap(String value, Collector<Tuple2<String,Integer>> collector) throws Exception {
            String[] s = value.split(" ");
            for (String word : s) {
                collector.collect(new Tuple2<>(word,1));
            }
        }
    }
}
