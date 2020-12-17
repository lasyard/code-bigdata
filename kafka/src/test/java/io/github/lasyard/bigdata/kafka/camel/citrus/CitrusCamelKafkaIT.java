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

package io.github.lasyard.bigdata.kafka.camel.citrus;

import com.consol.citrus.annotations.CitrusTest;
import com.consol.citrus.dsl.junit.JUnit4CitrusTestDesigner;
import org.apache.camel.component.kafka.KafkaConstants;
import org.junit.Test;

public class CitrusCamelKafkaIT extends JUnit4CitrusTestDesigner {
    @Test
    @CitrusTest
    public void testCamelKafka() {
        camel().context("kafkaTest").start();
        sleep(3000); // Wait receiver ready.
        send("writeKafka")
            .header(KafkaConstants.KEY, "test")
            .header(KafkaConstants.PARTITION_KEY, 0)
            .payload("Test in java.");
        receive("readKafka")
            .header(KafkaConstants.PARTITION, 0)
            .payload("Test in java.");
        camel().context("kafkaTest").stop();
    }
}
