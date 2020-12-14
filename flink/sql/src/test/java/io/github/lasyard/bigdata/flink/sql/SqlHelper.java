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

package io.github.lasyard.bigdata.flink.sql;

import lombok.Getter;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import javax.annotation.Nonnull;

public final class SqlHelper {
    @Getter
    private final StreamTableEnvironment tblEnv;

    public SqlHelper(StreamExecutionEnvironment env) {
        tblEnv = StreamTableEnvironment.create(env);
    }

    public DataStream<Event> toAppendStream(@Nonnull Table in) {
        final TableSchema schema = in.getSchema();
        final String[] columns = schema.getFieldNames();
        final int size = columns.length;
        DataStream<Row> stream = tblEnv.toAppendStream(in, Row.class);
        return stream.map((MapFunction<Row, Event>) value -> {
            Event res = new Event(size);
            for (int i = 0; i < size; i++) {
                String key = columns[i];
                res.put(key, value.getField(i));
            }
            return res;
        });
    }

    public DataStream<Event> toRetractStream(@Nonnull Table in) {
        final TableSchema schema = in.getSchema();
        final String[] columns = schema.getFieldNames();
        final int size = columns.length;
        DataStream<Tuple2<Boolean, Row>> stream = tblEnv.toRetractStream(in, Row.class);
        return stream.map((MapFunction<Tuple2<Boolean, Row>, Event>) value -> {
            Event res = new Event(size + 1);
            res.put("_ACTION", value.f0 ? "INS" : "DEL");
            for (int i = 0; i < size; i++) {
                String key = columns[i];
                res.put(key, value.f1.getField(i));
            }
            return res;
        });
    }

    public Table query(String sql) {
        return tblEnv.sqlQuery(sql);
    }

    public DataStream<Row> toRowStream(@Nonnull DataStream<Event> in, @Nonnull EventSchema schema) {
        final String[] keys = schema.getKeys();
        final TypeInformation<?>[] types = schema.getTypes();
        return in.map((MapFunction<Event, Row>) value -> {
            Row row = new Row(value.size());
            for (int i = 0; i < keys.length; i++) {
                row.setField(i, value.getOrDefault(keys[i], null));
            }
            return row;
        }).returns(new RowTypeInfo(types, keys));
    }

    public Table toTable(@Nonnull DataStream<Event> in, @Nonnull EventSchema schema) {
        return tblEnv.fromDataStream(toRowStream(in, schema));
    }
}
