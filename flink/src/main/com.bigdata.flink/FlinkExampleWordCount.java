package com.bigdata.flink;

import com.alibaba.fastjson2.JSON;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.util.Collector;

@Slf4j
public class FlinkExampleWordCount {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        executionEnvironment.setParallelism(1);

        //使用nc -lk 9999,进行数据流输入，如hello word hello flink
        DataStream<String> dataStream = executionEnvironment.socketTextStream("192.168.10.102", 9999);
        dataStream
                .flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
                             @Override
                             public void flatMap(String data, Collector<Tuple2<String, Integer>> collector) throws Exception {
                                 String[] split = data.split("\\s");
                                 log.info("split:{}", JSON.toJSON(split));
                                 for (String word : split) {
                                     collector.collect(Tuple2.of(word, 1));
                                 }
                             }
                         }
                )
                .keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
                    @Override
                    public String getKey(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                        return stringIntegerTuple2.f0;
                    }
                })
                .sum(1)
                .addSink(new PrintSinkFunction<Tuple2<String, Integer>>() {
                    @Override
                    public void invoke(Tuple2<String, Integer> value, Context context) throws Exception {
                        log.info(value.toString());
                        super.invoke(value, context);
                    }
                });

        executionEnvironment.execute();
    }
}
