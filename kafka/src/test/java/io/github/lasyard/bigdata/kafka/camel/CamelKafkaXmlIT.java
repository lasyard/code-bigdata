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

import io.github.lasyard.bigdata.kafka.KafkaHelper;
import io.github.lasyard.bigdata.kafka.KafkaProps;
import lombok.extern.slf4j.Slf4j;
import org.apache.camel.Endpoint;
import org.apache.camel.component.mock.MockEndpoint;
import org.apache.camel.test.spring.CamelSpringTestSupport;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.springframework.context.support.AbstractXmlApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import static org.apache.camel.builder.endpoint.StaticEndpointBuilders.mock;
import static org.apache.camel.builder.endpoint.StaticEndpointBuilders.ref;

@Slf4j
public class CamelKafkaXmlIT extends CamelSpringTestSupport {
    private static final KafkaHelper kafka = new KafkaHelper(KafkaProps.BOOTSTRAP_SERVERS);

    @BeforeClass
    public static void setupClass() {
    }

    @AfterClass
    public static void tearDownClass() {
        kafka.deleteTopic("test-camel-kafka");
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
    public AbstractXmlApplicationContext createApplicationContext() {
        return new ClassPathXmlApplicationContext("test-camel-kafka.xml");
    }

    @Test
    public void test() throws Exception {
        context().getRouteController().startAllRoutes();
        Thread.sleep(3000); // Wait receiver ready.
        String testString = RandomStringUtils.randomAlphabetic(16);
        Endpoint kafkaEnd = context().getEndpoint(ref("ed").getUri());
        template().sendBody(kafkaEnd, testString);
        MockEndpoint mock = context().getEndpoint(mock("read-from-kafka").getUri(), MockEndpoint.class);
        mock.expectedMessageCount(1);
        mock.message(0).body().isEqualTo(testString);
        mock.assertIsSatisfied();
    }
}
