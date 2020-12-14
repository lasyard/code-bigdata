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

package io.github.lasyard.bigdata.flink.helper;

import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class CollectSink implements SinkFunction<String> {
    private static final long serialVersionUID = 8363461737971569688L;

    // must be static
    private static final List<String> values = new LinkedList<>();

    public static void clear() {
        values.clear();
    }

    public static List<String> getValues() {
        return values;
    }

    public static void assertOutput(String... strings) {
        assertThat(getValues(), is(Arrays.asList(strings)));
    }

    @Override
    public synchronized void invoke(String value, Context ctx) {
        values.add(value);
    }
}
