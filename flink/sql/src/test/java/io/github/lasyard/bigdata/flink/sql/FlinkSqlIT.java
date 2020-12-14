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

import io.github.lasyard.bigdata.flink.helper.AlwaysEmitStrategy;
import io.github.lasyard.bigdata.flink.helper.CollectSink;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.types.Row;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import static org.apache.flink.table.api.Expressions.$;

public class FlinkSqlIT {
    @ClassRule
    public static MiniClusterWithClientResource cluster =
        new MiniClusterWithClientResource(new MiniClusterResourceConfiguration.Builder()
            .setNumberSlotsPerTaskManager(2)
            .setNumberTaskManagers(1)
            .build());

    private StreamExecutionEnvironment env;
    private SqlHelper helper;

    @Before
    public void setup() {
        CollectSink.clear();
        env = StreamExecutionEnvironment.getExecutionEnvironment()
            .disableOperatorChaining()
            .setParallelism(1);
        helper = new SqlHelper(env);
    }

    @Test
    public void testWhere() throws Exception {
        EventSchema schema = new EventSchema(
            new String[]{
                "id", "key"
            },
            new TypeInformation<?>[]{
                TypeInformation.of(Integer.class),
                TypeInformation.of(String.class)
            }
        );
        DataStream<Event> in = env.fromElements(
            schema.of(1, "a"),
            schema.of(2, "b"),
            schema.of(3, "c"),
            schema.of(4, "d"),
            schema.of(5, "e"),
            schema.of(6, "a"),
            schema.of(7, "c"),
            schema.of(8, "d"),
            schema.of(9, "f"),
            schema.of(10, "a"),
            schema.of(11, "e"),
            schema.of(12, "a")
        );
        Table tbl = helper.toTable(in, schema);
        Table res = helper.query("select id, key from " + tbl + " where key = 'a'");
        helper.toAppendStream(res)
            .map(Event::toString)
            .addSink(new CollectSink())
            .setParallelism(1);
        env.execute();
        CollectSink.assertOutput(
            "{id=1, key=a}",
            "{id=6, key=a}",
            "{id=10, key=a}",
            "{id=12, key=a}"
        );
    }

    @Test
    public void testGroup() throws Exception {
        EventSchema schema = new EventSchema(
            new String[]{
                "id", "key"
            },
            new TypeInformation<?>[]{
                TypeInformation.of(Integer.class),
                TypeInformation.of(String.class)
            }
        );
        DataStream<Event> in = env.fromElements(
            schema.of(1, "a"),
            schema.of(2, "b"),
            schema.of(3, "c"),
            schema.of(4, "d"),
            schema.of(5, "e"),
            schema.of(6, "a"),
            schema.of(7, "c"),
            schema.of(8, "d"),
            schema.of(9, "f"),
            schema.of(10, "a"),
            schema.of(11, "e"),
            schema.of(12, "a")
        );
        Table tbl = helper.toTable(in, schema);
        Table res = helper.query("select key, count(key) as cnt from " + tbl + " group by key");
        helper.toRetractStream(res)
            .map(Event::toString)
            .addSink(new CollectSink())
            .setParallelism(1);
        env.execute();
        CollectSink.assertOutput(
            "{_ACTION=INS, key=a, cnt=1}",
            "{_ACTION=INS, key=b, cnt=1}",
            "{_ACTION=INS, key=c, cnt=1}",
            "{_ACTION=INS, key=d, cnt=1}",
            "{_ACTION=INS, key=e, cnt=1}",
            "{_ACTION=DEL, key=a, cnt=1}",
            "{_ACTION=INS, key=a, cnt=2}",
            "{_ACTION=DEL, key=c, cnt=1}",
            "{_ACTION=INS, key=c, cnt=2}",
            "{_ACTION=DEL, key=d, cnt=1}",
            "{_ACTION=INS, key=d, cnt=2}",
            "{_ACTION=INS, key=f, cnt=1}",
            "{_ACTION=DEL, key=a, cnt=2}",
            "{_ACTION=INS, key=a, cnt=3}",
            "{_ACTION=DEL, key=e, cnt=1}",
            "{_ACTION=INS, key=e, cnt=2}",
            "{_ACTION=DEL, key=a, cnt=3}",
            "{_ACTION=INS, key=a, cnt=4}"
        );
    }

    @Test
    public void testCep() throws Exception {
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        EventSchema schema = new EventSchema(
            new String[]{
                "type",
                "name",
                "ts",
            },
            new TypeInformation<?>[]{
                TypeInformation.of(Integer.class),
                TypeInformation.of(String.class),
                TypeInformation.of(Long.class)
            }
        );
        DataStream<Event> in = env.fromElements(
            schema.of(1, "a", 1L),
            schema.of(1, "b", 2L),
            schema.of(1, "b", 3L),
            schema.of(3, "a", 4L),
            schema.of(3, "c", 5L),
            schema.of(3, "b", 6L),
            schema.of(1, "b", 7L),
            schema.of(2, "a", 8L),
            schema.of(3, "a", 9L),
            schema.of(3, "b", 10L),
            schema.of(1, "b", 11L),
            schema.of(1, "a", 12L)
        ).assignTimestampsAndWatermarks(
            new AlwaysEmitStrategy<>(event -> (Long) event.get("ts"))
        );
        DataStream<Row> rowStream = helper.toRowStream(in, schema);
        Table tbl = helper.getTblEnv().fromDataStream(
            rowStream,
            $("type"),
            $("name"),
            $("ts").rowtime()
        );
        Table res = helper.query(
            "SELECT t.aType, t.bType, t.a, t.b, t.aTs, t.bTs\n"
                + "FROM " + tbl + "\n"
                + "MATCH_RECOGNIZE (\n"
                + "  PARTITION BY type\n"
                + "  ORDER BY ts\n"
                + "  MEASURES A.type AS aType, B.type AS bType, A.name AS a, B.name AS b, A.ts AS aTs, B.ts AS bTs\n"
                + "  PATTERN (A B)\n"
                + "  DEFINE A AS name = 'a', B AS name = 'b'\n"
                + ") AS t"
        );
        helper.toAppendStream(res)
            .map(Event::toString)
            .addSink(new CollectSink());
        env.execute();
        CollectSink.assertOutput(
            "{aType=1, bType=1, a=a, b=b, aTs=1970-01-01T00:00:00.001, bTs=1970-01-01T00:00:00.002}",
            "{aType=3, bType=3, a=a, b=b, aTs=1970-01-01T00:00:00.009, bTs=1970-01-01T00:00:00.010}"
        );
    }
}
