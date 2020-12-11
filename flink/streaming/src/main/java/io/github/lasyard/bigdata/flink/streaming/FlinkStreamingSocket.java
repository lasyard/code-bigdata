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

package io.github.lasyard.bigdata.flink.streaming;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

public final class FlinkStreamingSocket {
    private FlinkStreamingSocket() {
    }

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().disableGenericTypes();
        DataStream<String> dataStream = env.socketTextStream("localhost", 12345, "\n");
        DataStream<Tuple2<String, Integer>> wordCount = dataStream
            .flatMap((String s, Collector<Tuple2<String, Integer>> collector) -> {
                for (String word : s.split("\\W+")) {
                    collector.collect(new Tuple2<>(word, 1));
                }
            })
            .returns(new TypeHint<Tuple2<String, Integer>>() {
            })
            .keyBy(t -> t.f0)
            .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
            .reduce((Tuple2<String, Integer> t1, Tuple2<String, Integer> t2) -> new Tuple2<>(
                t1.getField(0),
                (Integer) t1.getField(1) + (Integer) t2.getField(1)
            ));
        wordCount.print().setParallelism(1);
        env.execute();
    }
}
