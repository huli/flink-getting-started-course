package com.pluralsight.flink;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

public class WordCountWithCountWindow {

    public static void main(String[] args) throws Exception{

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.socketTextStream("localhost", 9999)
                .flatMap(new WordCountSplitter())
                .keyBy("word")
                .countWindow(3)
                .sum("count")
                .print();

        env.execute();
    }

    private static class WordCountSplitter implements org.apache.flink.api.common.functions.FlatMapFunction<String, WordCount> {
        @Override
        public void flatMap(String value, Collector<WordCount> out) throws Exception {
            for(String word: value.split(" ")){
                out.collect(new WordCount(word, 1));
            }
        }
    }

    public static class WordCount {
        public String word;
        public Integer count;

        public WordCount() {

        }

        public WordCount(String word, Integer count){
            this.word = word;
            this.count = count;
        }

        @Override
        public String toString() {
            return word + ": " + count;
        }
    }
}
