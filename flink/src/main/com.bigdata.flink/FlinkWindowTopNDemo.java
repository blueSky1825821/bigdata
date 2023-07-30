package com.bigdata.flink;

import lombok.*;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Comparator;
import java.util.Random;

import java.sql.Timestamp;

public class FlinkWindowTopNDemo {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.getConfig().setAutoWatermarkInterval(100);
        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);

        SingleOutputStreamOperator<UserEvent> eventStream = env.addSource(new CustomSouce())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<UserEvent>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<UserEvent>() {
                            @Override
                            public long extractTimestamp(UserEvent element, long
                                    recordTimestamp) {
                                return element.getTimestemp();
                            }
                        }));

        //按照访问url分区，求出每个 url 的访问量
        SingleOutputStreamOperator<UserViewCount> urlCountStream = eventStream
                .keyBy(data -> data.getUrl())
                //滚动窗口:5s间隔
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .aggregate(new NewUrlViewCountAgg(), new NewUrlViewCountResult());

        // 对结果中同一个窗口的统计数据，进行排序处理
        SingleOutputStreamOperator<String> result = urlCountStream
                .keyBy(data -> data.windowEnd)
                .process(new TopN(3));
        result.print("result");


        env.execute();
    }

    //自定义增量聚合
    public static class NewUrlViewCountAgg implements AggregateFunction<UserEvent, Long, Long> {
        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(UserEvent value, Long accumulator) {
            return accumulator + 1L;
        }

        @Override
        public Long getResult(Long accumulator) {
            return accumulator;
        }

        @Override
        public Long merge(Long a, Long b) {
            return null;
        }
    }

    //自定义全窗口函数，只需要包装窗口信息
    public static class NewUrlViewCountResult extends ProcessWindowFunction<Long, UserViewCount, String, TimeWindow> {

        @Override
        public void process(String url, ProcessWindowFunction<Long, UserViewCount, String, TimeWindow>.Context context, Iterable<Long> elements, Collector<UserViewCount> out) throws Exception {
            // 结合窗口信息，包装输出内容
            long currentWindowStartTimeStemp = context.window().getStart();
            long currentWindowEndTimeStemp = context.window().getEnd();
            out.collect(new UserViewCount(url, elements.iterator().next(), currentWindowStartTimeStemp, currentWindowEndTimeStemp));
        }
    }

    // 自定义处理函数，排序取 top n
    public static class TopN extends KeyedProcessFunction<Long, UserViewCount, String> {
        // 将 n 作为属性
        private Integer n;
        // 定义一个列表状态
        private ListState<UserViewCount> urlViewCountListState;

        public TopN(Integer n) {
            this.n = n;
        }

        public TopN() {
            super();
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            // 从环境中获取列表状态句柄
            urlViewCountListState = getRuntimeContext().getListState(
                    new ListStateDescriptor<UserViewCount>("url-view-count-list",
                            Types.POJO(UserViewCount.class)));
        }

        @Override
        public void processElement(UserViewCount value, KeyedProcessFunction<Long, UserViewCount, String>.Context ctx, Collector<String> out) throws Exception {
            // 将 count 数据添加到列表状态中，保存起来
            urlViewCountListState.add(value);

            // 注册 window end + 1ms 后的定时器，等待所有数据到齐开始排序
            ctx.timerService().registerEventTimeTimer(ctx.getCurrentKey() + 1);
        }

        @Override
        public void onTimer(long timestamp, KeyedProcessFunction<Long, UserViewCount, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
            // 将数据从列表状态变量中取出，放入 ArrayList，方便排序
            ArrayList<UserViewCount> urlViewCountArrayList = new ArrayList<>();
            for (UserViewCount urlViewCount : urlViewCountListState.get()) {
                urlViewCountArrayList.add(urlViewCount);
            }

            // 清空状态，释放资源
            urlViewCountListState.clear();

            // 排序
            urlViewCountArrayList.sort(new Comparator<UserViewCount>() {
                @Override
                public int compare(UserViewCount o1, UserViewCount o2) {
                    return o2.count.intValue() - o1.count.intValue();
                }
            });


            // 提取前两名，构建输出结果
            StringBuilder result = new StringBuilder();
            result.append("========================================\n");
            result.append("窗口结束时间：" + new Timestamp(timestamp - 1) + "\n");
            for (int i = 0; i < this.n; i++) {
                UserViewCount userViewCount = urlViewCountArrayList.get(i);
                if (null == userViewCount) {
                    break;
                }
                UserViewCount needUserView = userViewCount;
                String info = "No." + (i + 1) + " "
                        + "url：" + userViewCount.url + " "
                        + "浏览量：" + userViewCount.count + "\n";
                result.append(info);
            }
            result.append("========================================\n");
            out.collect(result.toString());

        }

    }



    @Data
    @NoArgsConstructor
    @ToString
    @AllArgsConstructor
    public static class UserEvent {
        private String userName;
        private String url;
        private Long timestemp;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @Builder
    public static class UserViewCount {
        public String url;
        public Long count;
        public Long windowStart;
        public Long windowEnd;

        @Override
        public String toString() {
            return "UserViewCount{" +
                    "url='" + url + '\'' +
                    ", count=" + count +
                    ", windowStart=" + new Timestamp(windowStart) +
                    ", windowEnd=" + new Timestamp(windowEnd) +
                    '}';
        }
    }

    public static class CustomSouce implements SourceFunction<UserEvent> {
        // 声明一个布尔变量，作为控制数据生成的标识位
        private Boolean running = true;

        @Override
        public void run(SourceContext<UserEvent> ctx) throws Exception {
            Random random = new Random(); // 在指定的数据集中随机选取数据
            String[] users = {"Mary", "Alice", "Bob", "Cary"};
            String[] urls = {"./home", "./cart", "./fav", "./prod?id=1",
                    "./prod?id=2"};
            while (running) {
                ctx.collect(new UserEvent(
                        users[random.nextInt(users.length)],
                        urls[random.nextInt(urls.length)],
                        Calendar .getInstance().getTimeInMillis()
                ));
                // 隔 200 ms生成一个点击事件，方便观测
                Thread.sleep(200);
            }
        }
        @Override
        public void cancel() {
            running = false;
        }

    }

}
