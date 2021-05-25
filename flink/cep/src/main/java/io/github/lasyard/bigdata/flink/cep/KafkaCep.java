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

package io.github.lasyard.bigdata.flink.cep;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.time.Duration;
import java.util.Properties;

public final class KafkaCep {
    private KafkaCep() {
    }

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("group.id", "test");
        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>(
            "test",
            new SimpleStringSchema(),
            properties
        );
        consumer.setStartFromEarliest();
        consumer.assignTimestampsAndWatermarks(
            WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofMillis(10))
        );
        DataStream<String> dataStream = env.addSource(consumer).setParallelism(3);
        KeyedStream<String, String> keyed = dataStream.keyBy(value -> value.substring(0, 1));
        Pattern<String, ?> pattern = Pattern.<String>begin("1").where(new SimpleCondition<String>() {
            private static final long serialVersionUID = 5132932755286546969L;

            @Override
            public boolean filter(String value) {
                if (value.length() >= 2) {
                    return value.charAt(1) == 'a';
                }
                return false;
            }
        }).times(2);
        PatternStream<String> patternStream = CEP.pattern(keyed, pattern);
        // the parallelism only can be 1 if the input stream is not keyed.
        DataStream<String> out = patternStream.select(map -> map.get("1").get(0)).setParallelism(5);
        FlinkKafkaProducer<String> producer = new FlinkKafkaProducer<>(
            "test-out",
            new SimpleStringSchema(),
            properties
        );
        out.addSink(producer).setParallelism(3);
        env.execute();
    }
}
