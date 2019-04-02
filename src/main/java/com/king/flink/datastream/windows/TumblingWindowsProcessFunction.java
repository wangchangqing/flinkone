package com.king.flink.datastream.windows;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.util.Collector;
import scala.Tuple2;

import java.util.Properties;

public class TumblingWindowsProcessFunction {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("group.id", "test");
        FlinkKafkaConsumer010<String> stringFlinkKafkaConsumer010 = new FlinkKafkaConsumer010<String>("jsontest", new SimpleStringSchema(), properties);
        stringFlinkKafkaConsumer010.setStartFromLatest();
        DataStreamSource<String> source = env.addSource(stringFlinkKafkaConsumer010);

        SingleOutputStreamOperator<String> process = source.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String s) throws Exception {
                String[] split = s.split(",");
                return new Tuple2<>(split[0], Integer.valueOf(split[1]));
            }
        }).keyBy(value -> value._1).timeWindow(Time.seconds(5)).process(new ProcessWindowFunction<Tuple2<String, Integer>, String, String, TimeWindow>() {
            @Override
            public void process(String key, Context context, Iterable<Tuple2<String, Integer>> input, Collector<String> collector) throws Exception {
                int sum = 0;
                for (Tuple2<String, Integer> item : input) {
                    sum+=item._2;
                }
                collector.collect(key+"   "+sum);
            }
        });
        process.print();
        env.execute("TumblingWindowsProcessFunction");
    }

}
