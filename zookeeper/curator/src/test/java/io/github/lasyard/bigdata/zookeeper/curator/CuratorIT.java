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

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.nio.charset.StandardCharsets;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class CuratorIT {
    private static final String TEST_NODE = "/test";

    private static CuratorFramework client;

    @BeforeClass
    public static void setupClass() throws Exception {
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
        client = CuratorFrameworkFactory.newClient("las1:2181", retryPolicy);
        client.start();
        if (client.checkExists().forPath(TEST_NODE) != null) {
            client.delete().forPath(TEST_NODE);
        }
    }

    @AfterClass
    public static void cleanupClass() throws Exception {
        if (client.checkExists().forPath(TEST_NODE) != null) {
            client.delete().forPath(TEST_NODE);
        }
        client.close();
    }

    @Test
    public void testReadWrite() throws Exception {
        client.create().forPath(TEST_NODE, "abc".getBytes(StandardCharsets.UTF_8));
        String r = new String(client.getData().forPath(TEST_NODE), StandardCharsets.UTF_8);
        assertThat(r, is("abc"));
    }
}
