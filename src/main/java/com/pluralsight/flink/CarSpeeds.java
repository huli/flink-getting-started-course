package com.pluralsight.flink;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class CarSpeeds {
    public static void main(String[] args) throws Exception{

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.socketTextStream("localhost", 9999)
                .map(new Speed())
                .keyBy(0)
                .flatMap(new AveragedSpeedValueState())
                .print();

        env.execute("Average speed since last speeding");
    }

    public static class Speed implements MapFunction<String, Tuple2<Integer, Double>> {
        @Override
        public Tuple2<Integer, Double> map(String value) throws Exception {

            // StateFunction only work on keyed streams, so we just set a key of 1
            return Tuple2.of(1, Double.parseDouble(value));
        }
    }

    private static class AveragedSpeedValueState extends RichFlatMapFunction<Tuple2<Integer, Double>, String> {

        private transient ValueState<Tuple2<Integer, Double>> countSumState;

        @Override
        public void flatMap(Tuple2<Integer, Double> value, Collector<String> out) throws Exception {

            Tuple2<Integer, Double> currentCountSum = countSumState.value();

            if(value.f1 >= 65 /* miles per hour */){
                out.collect(String.format(
                        "EXCEEDED! The average speed of the last %s car(s) was %s, your speed is %s",
                        currentCountSum.f0,
                        currentCountSum.f1 / currentCountSum.f0,
                        value.f1));
                countSumState.clear();
                currentCountSum = countSumState.value();
            }else{
                out.collect("Thank you for staying under the speed limit!");
            }

            currentCountSum.f0 += 1;
            currentCountSum.f1 += value.f1;

            countSumState.update(currentCountSum);
        }

        @Override
        public void open(Configuration config){

            ValueStateDescriptor<Tuple2<Integer, Double>> descriptor =
                    new ValueStateDescriptor<>("carAverageSpeed",
                            TypeInformation.of(
                                    new TypeHint<Tuple2<Integer, Double>>() {

                                    }),
                            Tuple2.of(0, 0.0) /* Is deprecated but do not know the best version at the moment */);

            countSumState = getRuntimeContext().getState(descriptor);
        }
    }
}
