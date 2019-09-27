package com.pluralsight.flink;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.twitter.TwitterSource;
import org.apache.flink.util.Collector;

import java.util.Properties;

public class TwitterTumblingWindow {

    public static void main(String[] args) throws Exception{

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties props = TwitterConfiguration.getProperties();

        env.addSource(new TwitterSource(props))
                .flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {

                    private transient ObjectMapper jsonParser;
                    @Override
                    public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {

                        if(jsonParser == null){
                            jsonParser = new ObjectMapper();
                        }

                        JsonNode jsonNode = jsonParser.readValue(value, JsonNode.class);
                        String language = jsonNode.has("lang")
                                ? jsonNode.get("lang").asText()
                                : "unknown";
                      out.collect(new Tuple2<>(language, 1));
                    }
                })
                .keyBy(0)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
                .sum(1)
                .print();

        env.execute();
    }

    static class TwitterConfiguration {

        public static Properties getProperties(){
            Properties props = new Properties();
            props.setProperty(TwitterSource.CONSUMER_KEY, "...");
            props.setProperty(TwitterSource.CONSUMER_SECRET, "...");
            props.setProperty(TwitterSource.TOKEN, "...");
            props.setProperty(TwitterSource.TOKEN_SECRET, "...");

            return props;
        }
    }
}
