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

package io.github.lasyard.bigdata.hadoop.io;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.AfterClass;
import org.junit.Test;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.UUID;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class HadoopIT {
    private static final Path PATH = new Path("test.txt");

    private static FileSystem openHdfs() throws IOException {
        System.setProperty("hadoop.home.dir", "/");
        Configuration configuration = new Configuration();
        configuration.addResource("server.xml");
        return FileSystem.get(configuration);
    }

    @AfterClass
    public static void tearDownClass() throws Exception {
        UserGroupInformation ugi = UserGroupInformation.createRemoteUser("root");
        ugi.doAs((PrivilegedExceptionAction<Void>) () -> {
            try (FileSystem fileSystem = openHdfs()) {
                if (fileSystem.exists(PATH)) {
                    fileSystem.delete(PATH, true);
                }
            }
            return null;
        });
    }

    @Test
    public void readWriteTest() throws Exception {
        UserGroupInformation ugi = UserGroupInformation.createRemoteUser("root");
        ugi.doAs((PrivilegedExceptionAction<Void>) () -> {
            try (FileSystem fileSystem = openHdfs()) {
                FSDataOutputStream out = fileSystem.create(PATH, true);
                String string = UUID.randomUUID().toString();
                out.writeUTF(string);
                out.close();
                FSDataInputStream in = fileSystem.open(PATH);
                assertThat(in.readUTF(), is(string));
                in.close();
            }
            return null;
        });
    }

    @Test(expected = FileAlreadyExistsException.class)
    public void reCreateTest() throws Exception {
        UserGroupInformation ugi = UserGroupInformation.createRemoteUser("root");
        ugi.doAs((PrivilegedExceptionAction<Void>) () -> {
            try (FileSystem fileSystem = openHdfs()) {
                FSDataOutputStream out = fileSystem.create(PATH, true);
                out.close();
                out = fileSystem.create(PATH, false);
                out.close();
            }
            return null;
        });
    }

    @Test(expected = FileNotFoundException.class)
    public void openNonExistTest() throws Exception {
        UserGroupInformation ugi = UserGroupInformation.createRemoteUser("root");
        ugi.doAs((PrivilegedExceptionAction<Void>) () -> {
            try (FileSystem fileSystem = openHdfs()) {
                FSDataOutputStream out = fileSystem.create(PATH, true);
                if (fileSystem.exists(PATH)) {
                    fileSystem.delete(PATH, true);
                }
                FSDataInputStream in = fileSystem.open(PATH);
                in.close();
            }
            return null;
        });
    }

    @Test
    public void appendTest() throws Exception {
        UserGroupInformation ugi = UserGroupInformation.createRemoteUser("root");
        ugi.doAs((PrivilegedExceptionAction<Void>) () -> {
            try (FileSystem fileSystem = openHdfs()) {
                FSDataOutputStream out = fileSystem.create(PATH, true);
                String string = UUID.randomUUID().toString();
                out.writeUTF(string);
                out.close();
                out = fileSystem.append(PATH);
                String string1 = UUID.randomUUID().toString();
                out.writeUTF(string1);
                out.close();
                FSDataInputStream in = fileSystem.open(PATH);
                assertThat(in.readUTF(), is(string));
                assertThat(in.readUTF(), is(string1));
                in.close();
            }
            return null;
        });
    }

    @Test
    public void makeQualifiedTest() throws Exception {
        try (FileSystem fileSystem = openHdfs()) {
            String root = fileSystem.getConf().get("fs.defaultFS");
            String user = System.getProperty("user.name");
            final String testPath = "test";
            Path path = fileSystem.makeQualified(new Path(testPath));
            assertThat(path.toString(), is(root + "/user/" + user + "/" + testPath));
        }
    }
}
