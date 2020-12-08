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

package io.github.lasyard.bigdata.zookeeper.curator;

import lombok.extern.slf4j.Slf4j;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.leader.LeaderSelector;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListenerAdapter;
import org.apache.curator.retry.ExponentialBackoffRetry;

import java.io.Closeable;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

@Slf4j
public class LeaderSelection extends LeaderSelectorListenerAdapter implements Closeable {
    private static final String TEST_NODE = "/test";

    private final String name;
    private final LeaderSelector selector;

    private LeaderSelection(CuratorFramework client) {
        this.name = UUID.randomUUID().toString();
        selector = new LeaderSelector(client, TEST_NODE, this);
        selector.autoRequeue();
    }

    public static void main(String[] args) {
        final RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
        CuratorFramework client = CuratorFrameworkFactory.newClient("las1:2181", retryPolicy);
        client.start();
        try (LeaderSelection ins = new LeaderSelection(client)) {
            ins.start();
            Thread.sleep(Integer.MAX_VALUE);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private void start() {
        selector.start();
        log.info("\"{}\" started.", name);
    }

    @Override
    public void close() {
        selector.close();
    }

    @Override
    public void takeLeadership(CuratorFramework curatorFramework) throws Exception {
        final int waitSeconds = (int) (5 * Math.random()) + 1;
        log.info("\"{}\" is now the leader. Waiting {} seconds...", name, waitSeconds);
        try {
            Thread.sleep(TimeUnit.SECONDS.toMillis(waitSeconds));
        } catch (InterruptedException e) {
            log.error("\"{}\" was interrupted.", name);
            Thread.currentThread().interrupt();
        } finally {
            log.info("\"{}\" relinquishing leadership.\n", name);
        }
    }
}
