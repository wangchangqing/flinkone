package com.king.flink.datastream.watermark;

import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Lists;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.util.Collector;

import javax.annotation.Nullable;
import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class WaterMarkTest {

    static class MyEvent{

        private String id;
        private long timeStamp;

        public MyEvent() {
        }

        public MyEvent(String id, long timeStamp) {
            this.id = id;
            this.timeStamp = timeStamp;
        }

        public String getId() {
            return id;
        }

        public void setId(String id) {
            this.id = id;
        }

        public long getTimeStamp() {
            return timeStamp;
        }

        public void setTimeStamp(long timeStamp) {
            this.timeStamp = timeStamp;
        }

        @Override
        public String toString() {
            return "MyEvent{" +
                    "timeStamp=" + timeStamp +
                    '}';
        }
    }

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("group.id", "test");
        FlinkKafkaConsumer010<String> source = new FlinkKafkaConsumer010<>("jsontest", new SimpleStringSchema(), properties);
        env.addSource(source).map(new MapFunction<String, MyEvent>() {
            @Override
            public MyEvent map(String s) throws Exception {
                MyEvent myEvent = JSONObject.parseObject(s, MyEvent.class);
                return myEvent;
            }
        }).assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<MyEvent>() {
            private long currentMaxTimestamp = 0l;
            private long maxOutOfOrderness = 10000l;

            @Nullable
            @Override
            public Watermark getCurrentWatermark() {
                return new Watermark(currentMaxTimestamp - maxOutOfOrderness);
            }

            @Override
            public long extractTimestamp(MyEvent myEvent, long l) {
                long timeStamp = myEvent.getTimeStamp();
                currentMaxTimestamp = Math.max(timeStamp, currentMaxTimestamp);
                SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
                long watermark = currentMaxTimestamp - maxOutOfOrderness;
                System.out.println("timestamp:" + timeStamp + "|" + format.format(timeStamp) + ",watermark" + watermark + "|" + format.format(watermark));
                return timeStamp;
            }
        }).keyBy(value -> value.getId()).window(TumblingEventTimeWindows.of(Time.seconds(3))).apply(new WindowFunction<MyEvent, String, String, TimeWindow>() {
            @Override
            public void apply(String key, TimeWindow timeWindow, Iterable<MyEvent> iterable, Collector<String> collector) throws Exception {
                List<MyEvent> myEvents = Lists.newArrayList(iterable);
                SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
                long start = timeWindow.getStart();
                long end = timeWindow.getEnd();
                String format1 = format.format(start);
                String format2 = format.format(end);
                collector.collect(key + "|" + myEvents + "|" + format1 + "|" + format2);
            }
        }).print();

        env.execute("watermark test");

    }

}
