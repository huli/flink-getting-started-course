package com.pluralsight.flink;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class AverageViews {

    public static void main(String[] args) throws Exception{

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.socketTextStream("localhost", 9999)
                .map(new MapFunction<String, Tuple3<String, Double, Integer>>() {
                    @Override
                    public Tuple3<String, Double, Integer> map(String value) throws Exception {

                        String [] values = value.split(",");
                        if(values.length == 2)
                            return new Tuple3<>(values[0], Double.parseDouble(values[1]), 1);

                        return null;
                    }
                })
                .keyBy(0)
                .reduce(new ReduceFunction<Tuple3<String, Double, Integer>>() {
                    @Override
                    public Tuple3<String, Double, Integer> reduce(
                            Tuple3<String, Double, Integer> value1,
                            Tuple3<String, Double, Integer> value2) throws Exception {

                        return new Tuple3<>(value1.f0, value1.f1 + value2.f1, value1.f2 + value2.f2);
                    }
                })
                .map(new MapFunction<Tuple3<String, Double, Integer>, Tuple2<String, Double>>() {
                    @Override
                    public Tuple2<String, Double> map(Tuple3<String, Double, Integer> value) throws Exception {
                        return new Tuple2<>(value.f0, value.f1 / value.f2);
                    }
                })
                .print();

        env.execute("Average views job");

    }

}
