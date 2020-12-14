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
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

@Slf4j
public class Cep2IT {
    private static final String TEST_STRING = "a1 a2 a3 a4 c5 b6 b7";

    @ClassRule
    public static MiniClusterWithClientResource cluster =
        new MiniClusterWithClientResource(new MiniClusterResourceConfiguration.Builder()
            .setNumberSlotsPerTaskManager(2)
            .setNumberTaskManagers(1)
            .build());

    @Rule
    public ExpectedException exception = ExpectedException.none();

    @Before
    public void setup() {
        CollectSink.clear();
    }

    @After
    public void tearDown() {
    }

    @Test
    public void test1() throws Exception {
        Pattern<String, ?> pattern = Pattern.<String>begin("1").where(new Condition("a")).times(1, 3).greedy()
            .followedBy("2").where(new Condition("b"));
        Helper.match(TEST_STRING, pattern);
        CollectSink.assertOutput(
            "a2 a3 a4 b6",
            "a3 a4 b6",
            "a4 b6"
        );
    }
}
