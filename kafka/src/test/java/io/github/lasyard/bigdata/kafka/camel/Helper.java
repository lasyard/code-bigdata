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

package io.github.lasyard.bigdata.kafka.camel;

import org.apache.camel.test.junit4.CamelTestSupport;

import javax.annotation.Nonnull;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

final class Helper {
    private Helper() {
    }

    static void testKafka(@Nonnull CamelTestSupport cts) throws Exception {
        cts.context().getRouteController().startAllRoutes();
        Thread.sleep(1000); // Wait the seda receiver ready
        String testString = Double.toString(Math.random());
        cts.template().sendBody(Constants.KAFKA_URI, testString);
        String msg = (String) cts.consumer().receiveBody(Constants.READ_URI);
        assertThat(msg, is(testString));
        cts.context().stop();
    }
}
