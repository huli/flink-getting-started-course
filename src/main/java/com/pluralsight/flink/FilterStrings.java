package com.pluralsight.flink;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.logging.Filter;

public class FilterStrings {

    public static void main(String [] args) throws Exception{

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> dataStream = env.socketTextStream("localhost", 9999)
                .filter(new StringFilter());

        dataStream.print();

        env.execute("Filtering Strings...");
    }

}
