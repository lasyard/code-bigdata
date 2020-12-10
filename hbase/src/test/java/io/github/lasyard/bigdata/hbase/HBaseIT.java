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

package io.github.lasyard.bigdata.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.Test;

import java.io.IOException;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class HBaseIT {
    private static final String TABLE_NAME = "tbl_test";
    private static final String COLUMN_FAMILY = "cf_default";

    private static Connection getConnection() throws IOException {
        System.setProperty("hadoop.home.dir", "/");
        Configuration configuration = HBaseConfiguration.create();
        configuration.addResource("hbase-server.xml");
        return ConnectionFactory.createConnection(configuration);
    }

    @AfterClass
    public static void tearDownClass() throws IOException {
        TableName tableName = TableName.valueOf(TABLE_NAME);
        try (Connection connection = getConnection()) {
            try (Admin admin = connection.getAdmin()) {
                if (admin.tableExists(tableName)) {
                    admin.disableTable(tableName);
                    admin.deleteTable(tableName);
                }
            }
        }
    }

    private void createTableIfNotExists() throws IOException {
        try (Connection connection = getConnection()) {
            try (Admin admin = connection.getAdmin()) {
                TableName tableName = TableName.valueOf(TABLE_NAME);
                if (admin.tableExists(tableName)) {
                    return;
                }
                TableDescriptor table = TableDescriptorBuilder.newBuilder(tableName)
                    .setColumnFamily(
                        ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(COLUMN_FAMILY))
                            .setCompressionType(Compression.Algorithm.NONE).build()
                    ).build();
                admin.createTable(table);
            }
        }
    }

    @Test
    public void createTableTest() throws IOException {
        createTableIfNotExists();
        try (Connection connection = getConnection()) {
            try (Admin admin = connection.getAdmin()) {
                assertTrue(admin.tableExists(TableName.valueOf(TABLE_NAME)));
            }
        }
    }

    @Test
    public void putGetTest() throws IOException {
        createTableIfNotExists();
        try (Connection connection = getConnection()) {
            TableName tableName = TableName.valueOf(TABLE_NAME);
            try (Table table = connection.getTable(TableName.valueOf(TABLE_NAME))) {
                byte[] cf = Bytes.toBytes(COLUMN_FAMILY);
                byte[] rowKey = Bytes.toBytes("row1");
                byte[] col1 = Bytes.toBytes("c1");
                byte[] col2 = Bytes.toBytes("c2");
                Put put = new Put(rowKey);
                put.addColumn(cf, col1, Bytes.toBytes(123));
                put.addColumn(cf, col2, Bytes.toBytes("Alice"));
                table.put(put);
                Get get = new Get(rowKey);
                get.addColumn(cf, col1);
                get.addColumn(cf, col2);
                Result result = table.get(get);
                assertThat(Bytes.toInt(result.getValue(cf, col1)), is(123));
                assertThat(Bytes.toString(result.getValue(cf, col2)), is("Alice"));
            }
        }
    }
}
