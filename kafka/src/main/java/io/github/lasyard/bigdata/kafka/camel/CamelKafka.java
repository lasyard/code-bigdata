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
import org.apache.camel.CamelContext;
import org.apache.camel.ConsumerTemplate;
import org.apache.camel.Exchange;
import org.apache.camel.ProducerTemplate;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.kafka.KafkaConstants;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.camel.impl.engine.DefaultConsumerTemplate;
import org.apache.camel.impl.engine.DefaultProducerTemplate;

import java.util.Map;

@Slf4j
public final class CamelKafka {
    private CamelKafka() {
    }

    public static void main(String[] args) throws Exception {
        CamelContext context = new DefaultCamelContext();
        context.addRoutes(new RouteBuilder() {
            @Override
            public void configure() {
                from(Constants.WRITE_URI).process(exchange -> {
                    exchange.getIn().setHeader(KafkaConstants.PARTITION_KEY, 0);
                    exchange.getIn().setHeader(KafkaConstants.KEY, "test");
                }).to(Constants.KAFKA_URI);
                from(Constants.KAFKA_URI).process(exchange -> {
                    for (Map.Entry<String, Object> entry : exchange.getIn().getHeaders().entrySet()) {
                        log.info("{}: {}", entry.getKey(), entry.getValue().toString());
                    }
                }).to(Constants.READ_URI);
            }
        });
        ProducerTemplate producer = new DefaultProducerTemplate(context);
        ConsumerTemplate consumer = new DefaultConsumerTemplate(context);
        context.start();
        producer.start();
        consumer.start();
        String testString = "Test kafka with camel";
        producer.sendBody(Constants.WRITE_URI, testString);
        Exchange exchange = consumer.receive(Constants.READ_URI);
        log.info("Receive message: {}.", exchange.getIn().getBody());
        log.info("with headers: {}.", exchange.getIn().getHeaders());
        producer.stop();
        consumer.stop();
        context.stop();
    }
}
