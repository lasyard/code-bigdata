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

package io.github.lasyard.bigdata.flink.dataset;

import org.apache.commons.io.IOUtils;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import java.nio.charset.StandardCharsets;

public final class FlinkDataSet {
    private FlinkDataSet() {
    }

    public static void main(String[] args) throws Exception {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().disableGenericTypes();
        DataSet<String> text = env.fromCollection(IOUtils.readLines(
            FlinkDataSet.class.getResourceAsStream("/data.txt"), StandardCharsets.UTF_8
        ));
        DataSet<Tuple2<String, Integer>> counts = text
            .flatMap((String s, Collector<Tuple2<String, Integer>> collector) -> {
                String[] tokens = s.split("\\W+");
                for (String token : tokens) {
                    if (token.length() > 0) {
                        collector.collect(new Tuple2<>(token, 1));
                    }
                }
            })
            .returns(new TypeHint<Tuple2<String, Integer>>() {
            })
            .groupBy(0)
            .sum(1);
        counts.print();
    }
}
