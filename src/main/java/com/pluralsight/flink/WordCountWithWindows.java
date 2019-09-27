package com.pluralsight.flink;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

public class WordCountWithWindows {

    public static void main(String[] args) throws Exception{

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.socketTextStream("localhost", 9999)
                .flatMap(new WordCountSplitter())
                .keyBy(0)
                // Use non overlapping tumpling window
                // .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
                // Use overlapping sliding window
                .window(SlidingProcessingTimeWindows.of(Time.seconds(30), Time.seconds(5)))
                .sum(1)
                .print();

        env.execute();
    }

    private static class WordCountSplitter implements org.apache.flink.api.common.functions.FlatMapFunction<String, Tuple2<String, Integer>> {
        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> collector) throws Exception {

            String [] words = value.split(" ");
            for(String word: words)
            {
                collector.collect(new Tuple2<>(word.trim(), 1));
            }
        }
    }
}
