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
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
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
import org.junit.Test;

import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

@Slf4j
public class KafkaIT {
    private static final String BOOTSTRAP_SERVERS = "las1:9092,las2:9092,las3:9092";
    private static final String TOPIC = "test";

    private static AdminClient getAdminClient() {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(AdminClientConfig.CLIENT_ID_CONFIG, "KafkaIT");
        return AdminClient.create(props);
    }

    private static void createTopic() {
        try (AdminClient admin = getAdminClient()) {
            admin.createTopics(Collections.singletonList(new NewTopic(TOPIC, 3, (short) 1)));
        }
    }

    @AfterClass
    public static void tearDown() {
        try (AdminClient admin = getAdminClient()) {
            admin.deleteTopics(Collections.singletonList(TOPIC));
        }
    }

    @Test
    public void listTopics() throws Exception {
        createTopic();
        try (AdminClient admin = getAdminClient()) {
            ListTopicsResult result = admin.listTopics();
            Collection<String> topics = result.names().get();
            assertThat(topics, hasItem(TOPIC));
        }
    }

    @Test
    public void testTxRx() throws Exception {
        createTopic();
        ExecutorService executor = Executors.newSingleThreadExecutor();
        final Future<List<ConsumerRecord<Integer, String>>> records = executor.submit(() -> {
            Properties consumerProps = new Properties();
            consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
            consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "one");
            consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class.getName());
            consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
            try (Consumer<Integer, String> consumer = new KafkaConsumer<>(consumerProps)) {
                consumer.subscribe(Collections.singletonList(TOPIC));
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
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        try (Producer<Integer, String> producer = new KafkaProducer<>(producerProps)) {
            for (int count = 0; count < 10; count++) {
                String value = String.format("Hello %02d", count);
                producer.send(new ProducerRecord<>(TOPIC, count, value));
            }
        }
        List<ConsumerRecord<Integer, String>> recordList = records.get();
        for (ConsumerRecord<Integer, String> record : recordList) {
            assertThat(record.value(), is(String.format("Hello %02d", record.key())));
            log.info(record.value());
        }
    }
}
