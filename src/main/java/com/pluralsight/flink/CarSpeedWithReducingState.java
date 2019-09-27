package com.pluralsight.flink;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class CarSpeedWithReducingState {
    public static void main(String[] args) throws Exception{

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.socketTextStream("localhost", 9999)
                .map(new CarSpeeds.Speed())
                .keyBy(0)
                .flatMap(new AveragedSpeedReducingState())
                .print();

        env.execute("Average speed since last speeding");
    }

    private static class Speed implements MapFunction<String, Tuple2<Integer, Double>> {
        @Override
        public Tuple2<Integer, Double> map(String value) throws Exception {

            // StateFunction only work on keyed streams, so we just set a key of 1
            return Tuple2.of(1, Double.parseDouble(value));
        }
    }

    private static class AveragedSpeedReducingState extends org.apache.flink.api.common.functions.RichFlatMapFunction<Tuple2<Integer, Double>, String> {

        private transient ValueState<Integer> numberOfCarsState;
        private transient ReducingState<Double> totalSpeedOfCarsState;

        @Override
        public void flatMap(Tuple2<Integer, Double> value, Collector<String> out) throws Exception {

            if(value.f1 >= 65){

                Double totalSpeedOfCars = totalSpeedOfCarsState.get();
                Integer numberOfCars = numberOfCarsState.value();

                out.collect(String.format(
                        "EXCEEDED! The average speed of the last %s car(s) was %s, your speed is %s",
                        numberOfCars,
                        totalSpeedOfCars / numberOfCars,
                        value.f1));

                totalSpeedOfCarsState.clear();
                numberOfCarsState.clear();

            }else{
                out.collect("Thanks, easy rider.");
            }

            numberOfCarsState.update(numberOfCarsState.value() + 1);
            totalSpeedOfCarsState.add(value.f1);
        }

        @Override
        public void open(Configuration conf){

            ValueStateDescriptor<Integer> descriptor = new ValueStateDescriptor<Integer>(
                    "carCount", Integer.class, 0
            );
            numberOfCarsState = getRuntimeContext().getState(descriptor);

            ReducingStateDescriptor<Double> reducingStateDescriptor = new ReducingStateDescriptor<Double>(
                    "totalSpeed",
                    new ReduceFunction<Double>() {
                        @Override
                        public Double reduce(Double value1, Double value2) throws Exception {
                            return value1 + value2;
                        }
                    }, Double.class
            );
            totalSpeedOfCarsState = getRuntimeContext().getReducingState(reducingStateDescriptor);
        }
    }
}
