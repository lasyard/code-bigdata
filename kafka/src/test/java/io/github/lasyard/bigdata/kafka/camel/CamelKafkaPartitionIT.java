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

import com.google.common.collect.ImmutableMap;
import io.github.lasyard.bigdata.kafka.KafkaHelper;
import io.github.lasyard.bigdata.kafka.KafkaProps;
import lombok.extern.slf4j.Slf4j;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.kafka.KafkaConstants;
import org.apache.camel.component.mock.MockEndpoint;
import org.apache.camel.test.junit4.CamelTestSupport;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Map;

import static org.apache.camel.builder.endpoint.StaticEndpointBuilders.kafka;
import static org.apache.camel.builder.endpoint.StaticEndpointBuilders.mock;

@Slf4j
public class CamelKafkaPartitionIT extends CamelTestSupport {
    private static final KafkaHelper kafka = new KafkaHelper(KafkaProps.BOOTSTRAP_SERVERS);
    private static String topic;
    private static String kafkaUri;
    private static String mockUri;

    @BeforeClass
    public static void setupClass() {
        topic = RandomStringUtils.randomAlphabetic(8);
        kafkaUri = kafka(topic).brokers(KafkaProps.BOOTSTRAP_SERVERS).getUri();
        mockUri = mock("read-from-kafka").getUri();
    }

    @AfterClass
    public static void tearDownClass() {
        kafka.deleteTopic(topic);
    }

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
                from(kafkaUri).noAutoStartup()
                    .process(exchange -> {
                            for (Map.Entry<String, Object> entry : exchange.getIn().getHeaders().entrySet()) {
                                log.info("{}: {}", entry.getKey(), entry.getValue().toString());
                            }
                        }
                    )
                    .to(mockUri);
            }
        };
    }

    @Test
    public void test() throws Exception {
        context().getRouteController().startAllRoutes();
        Thread.sleep(3000); // Wait receiver ready.
        String testString = RandomStringUtils.randomAlphabetic(16);
        template().sendBodyAndHeaders(kafkaUri, testString, ImmutableMap.of(
            KafkaConstants.PARTITION_KEY, 0,
            KafkaConstants.KEY, "test"
        ));
        MockEndpoint mock = context().getEndpoint(mockUri, MockEndpoint.class);
        mock.expectedMessageCount(1);
        mock.message(0).header(KafkaConstants.PARTITION_KEY).isEqualTo(0);
        mock.message(0).header(KafkaConstants.KEY).isEqualTo("test");
        mock.message(0).body().isEqualTo(testString);
        mock.assertIsSatisfied();
    }
}
