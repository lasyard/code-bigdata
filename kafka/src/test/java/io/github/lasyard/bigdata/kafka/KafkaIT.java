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

package io.github.lasyard.bigdata.kafka;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.time.Duration;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

@Slf4j
public class KafkaIT {
    private static final KafkaHelper kafka = new KafkaHelper(KafkaProps.BOOTSTRAP_SERVERS);
    private static String topic;

    @BeforeClass
    public static void setupClass() {
        topic = RandomStringUtils.randomAlphabetic(8);
        kafka.createTopic(topic, 3, (short) 1);
    }

    @AfterClass
    public static void tearDownClass() {
        kafka.deleteTopic(topic);
    }

    @Test
    public void testListTopics() throws Exception {
        Set<String> topics = kafka.listTopics();
        assertThat(topics, hasItem(topic));
    }

    @Test
    public void testTxRx() throws Exception {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        final Future<List<ConsumerRecord<Integer, String>>> records = executor.submit(() -> {
            Properties consumerProps = new Properties();
            consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
            consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "one");
            consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class.getName());
            consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
            try (Consumer<Integer, String> consumer = new KafkaConsumer<>(consumerProps)) {
                consumer.subscribe(Collections.singletonList(topic));
                List<ConsumerRecord<Integer, String>> recordList = new LinkedList<>();
                while (recordList.size() < 10) {
                    ConsumerRecords<Integer, String> recs = consumer.poll(Duration.ofMillis(100));
                    for (ConsumerRecord<Integer, String> rec : recs) {
                        recordList.add(rec);
                    }
                }
                return recordList;
            }
        });
        Thread.sleep(1000); // Wait the consumer ready.
        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        try (Producer<Integer, String> producer = new KafkaProducer<>(producerProps)) {
            for (int count = 0; count < 10; count++) {
                String value = String.format("Hello %02d", count);
                producer.send(new ProducerRecord<>(topic, count, value));
            }
        }
        List<ConsumerRecord<Integer, String>> recordList = records.get();
        for (ConsumerRecord<Integer, String> record : recordList) {
            assertThat(record.value(), is(String.format("Hello %02d", record.key())));
            log.info(record.value());
        }
    }
}
