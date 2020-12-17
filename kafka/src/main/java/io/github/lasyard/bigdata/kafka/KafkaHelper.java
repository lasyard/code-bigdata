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

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;

import java.util.Collection;
import java.util.Collections;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;

@Slf4j
@RequiredArgsConstructor
public class KafkaHelper {
    @Getter
    private final String bootstrapServers;

    public AdminClient getAdminClient() {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(AdminClientConfig.CLIENT_ID_CONFIG, getClass().getSimpleName());
        return AdminClient.create(props);
    }

    public void createTopic(String topic, int partitions, short replicas) {
        try (AdminClient admin = getAdminClient()) {
            admin.createTopics(Collections.singletonList(new NewTopic(topic, partitions, replicas)));
        }
    }

    public Set<String> listTopics() throws InterruptedException, ExecutionException {
        try (AdminClient admin = getAdminClient()) {
            return admin.listTopics().names().get();
        }
    }

    public void deleteTopics(Collection<String> topics) {
        try (AdminClient admin = getAdminClient()) {
            admin.deleteTopics(topics);
        }
        log.debug("Deleted topics \"{}\"", topics);
    }

    public void deleteTopic(String topic) {
        deleteTopics(Collections.singletonList(topic));
    }
}
