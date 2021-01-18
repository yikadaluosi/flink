package com.byynb.flink.day01;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @auther byy
 * @create 2021-01-18-11:33
 */
public class f2BundedStreamWC {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);
        DataStreamSource<String> lineDS = env.readTextFile("E:\\project\\byynb\\flink\\input");
        SingleOutputStreamOperator<Tuple2<String, Integer>> word2OneDS = lineDS.flatMap(new f1wc.line2TupleFlatMap());
        KeyedStream<Tuple2<String, Integer>, Object> groupDS = word2OneDS.keyBy((KeySelector<Tuple2<String, Integer>, Object>) value -> value.f0);
        SingleOutputStreamOperator<Tuple2<String, Integer>> result = groupDS.sum(1);
        result.print();
        env.execute();

    }
}
