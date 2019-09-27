package com.pluralsight.flink;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

public class MaxViewTimeWithSessionWindow {

    public static void main(String[] args) throws Exception{

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.socketTextStream("localhost", 9999)
                .map(new MapFunction<String, Tuple3<String, String, Double>>() {
                    @Override
                    public Tuple3<String, String, Double> map(String value) throws Exception {

                        String [] fields = value.split(",");
                        if(fields.length == 3){
                            return new Tuple3<>(fields[0], fields[1], Double.parseDouble(fields[2]));
                        }

                        return null;
                    }
                })
                .keyBy(0, 1)
                .window(ProcessingTimeSessionWindows.withGap(Time.seconds(10)))
                .max(2)
                .print();

        env.execute();
    }
}
