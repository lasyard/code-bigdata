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

import io.github.lasyard.bigdata.flink.helper.CollectSink;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import javax.annotation.Nonnull;

final class Helper {
    private Helper() {
    }

    static void match(@Nonnull String input, Pattern<String, ?> pattern) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment()
            .setParallelism(1);
        env.getConfig().disableGenericTypes();
        DataStream<String> dataStream = env.fromCollection(Arrays.asList(input.split(" ")));
        PatternStream<String> patternStream = CEP.pattern(dataStream, pattern);
        DataStream<String> result = patternStream.select(new PatternSelectFunction<String, String>() {
            private static final long serialVersionUID = -5708246944590876484L;

            @Override
            public String select(Map<String, List<String>> map) {
                StringBuilder b = new StringBuilder();
                for (Map.Entry<String, List<String>> entry : map.entrySet()) {
                    for (String o : entry.getValue()) {
                        if (b.length() > 0) {
                            b.append(" ");
                        }
                        b.append(o);
                    }
                }
                return b.toString();
            }
        });
        result.addSink(new CollectSink());
        env.execute();
    }
}
