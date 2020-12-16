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

import lombok.extern.slf4j.Slf4j;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.test.junit4.CamelTestSupport;
import org.junit.Test;

@Slf4j
public class CamelKafkaJavaIT extends CamelTestSupport {
    @Override
    public void setUp() throws Exception {
        super.setUp();
    }

    @Override
    public void tearDown() throws Exception {
        log.info("Test done. Tear down.");
        super.tearDown();
    }

    @Override
    public RouteBuilder createRouteBuilder() {
        return new RouteBuilder() {
            @Override
            public void configure() {
                from(Constants.KAFKA_URI)
                    .autoStartup(false)
                    .process(exchange -> {
                        String msg = exchange.getIn().getBody().toString();
                        log.info("Receive message: {}.", msg);
                    })
                    .to(Constants.READ_URI);
            }
        };
    }

    @Test
    public void kafkaTest() throws Exception {
        Helper.testKafka(this);
    }
}
