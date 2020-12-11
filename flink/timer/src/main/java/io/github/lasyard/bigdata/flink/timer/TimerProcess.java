/*
 * Copyright 2020 lasyard@github.com
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.github.lasyard.bigdata.flink.timer;

import com.opencsv.bean.CsvToBeanBuilder;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkGeneratorSupplier;
import org.apache.flink.api.common.eventtime.WatermarkOutput;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.util.List;

@Slf4j
public final class TimerProcess {
    private TimerProcess() {
    }

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment()
            .setParallelism(1);
        env.getConfig().disableGenericTypes();
        List<Event> events = new CsvToBeanBuilder<Event>(new InputStreamReader(
            TimerProcess.class.getResourceAsStream("/data.csv"),
            StandardCharsets.UTF_8
        ))
            .withType(Event.class)
            .build()
            .parse();
        DataStream<Event> dataStream = env.fromCollection(events).assignTimestampsAndWatermarks(
            WatermarkStrategy.forGenerator(
                (WatermarkGeneratorSupplier<Event>) context -> new WatermarkGenerator<Event>() {
                    @Override
                    public void onEvent(Event event, long eventTimestamp, WatermarkOutput output) {
                        output.emitWatermark(new Watermark(eventTimestamp));
                    }

                    @Override
                    public void onPeriodicEmit(WatermarkOutput output) {
                    }
                })
                .withTimestampAssigner((event, timestamp) -> event.getTimestamp().getTime())
        );
        DataStream<Tuple2<Event, Timestamp>> result = dataStream
            .keyBy(Event::getType)
            .process(new KeyedProcessFunction<String, Event, Tuple2<Event, Timestamp>>() {
                private static final long serialVersionUID = 5037322971661709352L;

                private transient ValueState<Event> eventState;

                @Override
                public void open(Configuration config) {
                    ValueStateDescriptor<Event> descriptor = new ValueStateDescriptor<>(
                        "event",
                        TypeInformation.of(Event.class)
                    );
                    eventState = getRuntimeContext().getState(descriptor);
                }

                @Override
                public void processElement(
                    Event event,
                    Context context,
                    Collector<Tuple2<Event, Timestamp>> out
                ) throws Exception {
                    long current = context.timestamp();
                    Timestamp timestamp = new Timestamp(current);
                    Timestamp watermark = new Timestamp(context.timerService().currentWatermark());
                    log.info(
                        "Current watermark: " + watermark
                            + ", Current timestamp: " + timestamp
                            + ", Current event: " + event
                    );
                    if (event.getAmount() == 200) {
                        context.timerService().registerEventTimeTimer(event.getTimestamp().getTime() + 3000L);
                        eventState.update(event);
                    } else {
                        out.collect(new Tuple2<>(event, timestamp));
                    }
                }

                @Override
                public void onTimer(
                    long timestamp,
                    OnTimerContext ctx,
                    Collector<Tuple2<Event, Timestamp>> out
                ) throws Exception {
                    out.collect(new Tuple2<>(eventState.value(), new Timestamp(timestamp)));
                }
            });
        result.print();
        env.execute();
    }
}
