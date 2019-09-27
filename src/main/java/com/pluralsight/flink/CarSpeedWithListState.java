package com.pluralsight.flink;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Iterator;

public class CarSpeedWithListState {
    public static void main(String[] args) throws Exception{

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.socketTextStream("localhost", 9999)
                .map(new CarSpeeds.Speed())
                .keyBy(0)
                .flatMap(new AveragedSpeedListState())
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

    private static class AveragedSpeedListState extends RichFlatMapFunction<Tuple2<Integer, Double>, String> {

        private transient ListState<Double> speedListState;

        @Override
        public void flatMap(Tuple2<Integer, Double> value, Collector<String> out) throws Exception {

                if(value.f1 >= 65){

                    Iterable<Double> carSpeeds = speedListState.get();
                    int count = 0;
                    int sum = 0;
                    for(Double speed: carSpeeds){
                        count++;
                        sum += speed;
                    }
                    out.collect(String.format(
                            "EXCEEDED! The average speed of the last %s car(s) was %s, your speed is %s",
                            count,
                            sum / count,
                            value.f1));

                    speedListState.clear();;

                }else{
                    out.collect("Thanks for taking it easy...");
                }

                speedListState.add(value.f1);
        }

        @Override
        public void open(Configuration config){
            ListStateDescriptor<Double> descriptor = new ListStateDescriptor<Double>(
                    "carSpeeds", Double.class);

            speedListState = getRuntimeContext().getListState(descriptor);
        }
    }
}
