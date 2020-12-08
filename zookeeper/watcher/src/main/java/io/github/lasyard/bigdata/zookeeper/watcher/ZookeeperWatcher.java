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

package io.github.lasyard.bigdata.zookeeper.watcher;

import lombok.extern.slf4j.Slf4j;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import javax.annotation.Nonnull;

@Slf4j
public class ZookeeperWatcher implements Runnable, Watcher {
    private static final String TEST_NODE = "/test";

    private final ZooKeeper zooKeeper;

    private ZookeeperWatcher() throws IOException {
        zooKeeper = new ZooKeeper("las1:2181", 3000, this);
    }

    private static void logStat(@Nonnull Stat stat) {
        log.info("Stat of node:");
        log.info("Czxid = {}", stat.getCzxid());
        log.info("Mzxid = {}", stat.getMzxid());
        log.info("Pzxid = {}", stat.getPzxid());
        log.info("Ctime = {}", stat.getCtime());
        log.info("Mtime = {}", stat.getMtime());
        log.info("Cversion = {}", stat.getCversion());
        log.info("Version = {}", stat.getVersion());
        log.info("Aversion = {}", stat.getAversion());
        log.info("DataLength = {}", stat.getDataLength());
        log.info("NumChildren = {}", stat.getNumChildren());
        log.info("EphemeralOwner = {}", stat.getEphemeralOwner());
    }

    public static void main(String[] args) {
        try {
            new ZookeeperWatcher().run();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void run() {
        try {
            synchronized (this) {
                wait();
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void process(WatchedEvent event) {
        log.info("Event received.");
        Event.KeeperState state = event.getState();
        log.info("Event state: {}.", state);
        Event.EventType type = event.getType();
        log.info("Event type: {}.", type);
        if (state == Event.KeeperState.SyncConnected) {
            log.info("Event state: SyncConnected.");
            try {
                String path = event.getPath();
                switch (type) {
                    case None:
                        log.info("Event type: None.");
                        break;
                    case NodeCreated:
                        log.info("Node \"{}\" created.", path);
                        break;
                    case NodeDeleted:
                        log.info("Node \"{}\" deleted.", path);
                        break;
                    case NodeDataChanged:
                        log.info("Data of node \"{}\" changed.", path);
                        break;
                    case NodeChildrenChanged:
                        log.info("Children of node \"{}\" changed.", path);
                        break;
                    default:
                        log.info("Unknown event type.");
                        break;
                }
                Stat stat = zooKeeper.exists(TEST_NODE, true);
                if (stat != null) {
                    log.info("Node \"{}\" exists.", TEST_NODE);
                    if (path != null) {
                        logNode(path);
                    }
                }
            } catch (KeeperException | InterruptedException e) {
                e.printStackTrace();
            }
        } else if (state == Event.KeeperState.Expired) {
            log.info("Event state: Expired.");
        }
    }

    private void logNode(String path) throws KeeperException, InterruptedException {
        Stat stat = new Stat();
        byte[] data = zooKeeper.getData(path, false, stat);
        if (data != null) {
            log.info("Data = {}", new String(data, StandardCharsets.UTF_8));
        }
        logStat(stat);
        List<String> children = zooKeeper.getChildren(path, true);
        if (children.size() > 0) {
            log.info("Children: {}", String.join(" ", children));
        }
    }
}
