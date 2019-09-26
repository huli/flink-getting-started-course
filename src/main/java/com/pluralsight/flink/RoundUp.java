package com.pluralsight.flink;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class RoundUp {

    public static void main(String[] args) throws Exception{

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.socketTextStream("localhost", 9999)
                .filter(new FilterFunction<String>() {
                    @Override
                    public boolean filter(String value) throws Exception {

                        try{
                            Double.parseDouble(value);
                            return true;
                        }catch(Exception ex){
                        }
                        return false;
                    }
                })
                .map(new MapFunction<String, Long>() {
                    @Override
                    public Long map(String value) throws Exception {

                        double d = Double.parseDouble(value.trim());
                        return Math.round(d);
                    }
                })
                .print();

        env.execute("Filter and round strings");
    }
}
