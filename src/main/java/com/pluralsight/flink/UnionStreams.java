package com.pluralsight.flink;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class UnionStreams {
    public static void main(String[] args) throws Exception{

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> stream1 = env.socketTextStream("localhost", 9000);
        DataStream<String> stream2 = env.socketTextStream("localhost", 8000);

        stream1.union(stream2)
                .print();

        env.execute();
    }
}
