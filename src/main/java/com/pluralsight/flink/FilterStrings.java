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

    static class StringFilter implements org.apache.flink.api.common.functions.FilterFunction<String> {
        @Override
        public boolean filter(String value) throws Exception {

            try{
                Double.parseDouble(value.trim());
                return true;
            }catch(Exception ex){
                return false;
            }

        }
    }
}
