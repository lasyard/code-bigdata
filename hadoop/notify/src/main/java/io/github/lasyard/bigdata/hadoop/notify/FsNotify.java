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

package io.github.lasyard.bigdata.hadoop.notify;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSInotifyEventInputStream;
import org.apache.hadoop.hdfs.client.HdfsAdmin;
import org.apache.hadoop.hdfs.inotify.Event;
import org.apache.hadoop.hdfs.inotify.EventBatch;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;
import java.net.URI;
import java.security.PrivilegedExceptionAction;

@Slf4j
public final class FsNotify {
    private FsNotify() {
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        UserGroupInformation ugi = UserGroupInformation.createRemoteUser("root");
        ugi.doAs((PrivilegedExceptionAction<Void>) () -> {
            System.setProperty("hadoop.home.dir", "/");
            Configuration configuration = new Configuration();
            configuration.addResource("server.xml");
            HdfsAdmin admin = new HdfsAdmin(URI.create(configuration.get("fs.defaultFS")), configuration);
            DFSInotifyEventInputStream stream = admin.getInotifyEventStream();
            while (true) {
                EventBatch events = stream.take();
                for (Event event : events.getEvents()) {
                    log.info("Event type: " + event.getEventType());
                    switch (event.getEventType()) {
                        case CREATE:
                            Event.CreateEvent createEvent = (Event.CreateEvent) event;
                            log.info("\tPath: " + createEvent.getPath());
                            log.info("\tOwner:" + createEvent.getOwnerName());
                            break;
                        case RENAME:
                            Event.RenameEvent renameEvent = (Event.RenameEvent) event;
                            log.info("\tSrcPath: " + renameEvent.getSrcPath());
                            log.info("\tDstPath: " + renameEvent.getDstPath());
                            break;
                        case CLOSE:
                            Event.CloseEvent closeEvent = (Event.CloseEvent) event;
                            log.info("\tPath: " + closeEvent.getPath());
                            break;
                        case UNLINK:
                            Event.UnlinkEvent unlinkEvent = (Event.UnlinkEvent) event;
                            log.info("\tPath: " + unlinkEvent.getPath());
                            break;
                        default:
                            break;
                    }
                }
            }
        });
    }
}
