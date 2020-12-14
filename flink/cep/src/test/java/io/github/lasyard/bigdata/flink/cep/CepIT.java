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

package io.github.lasyard.bigdata.flink.cep;

import io.github.lasyard.bigdata.flink.helper.CollectSink;
import org.apache.flink.cep.nfa.aftermatch.AfterMatchSkipStrategy;
import org.apache.flink.cep.pattern.MalformedPatternException;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class CepIT {
    private static final String TEST_STRING = "- a1 b1 b2 - a2 - b3 c1 - a3 - - c2 b4 b5 - b6 - c3 - b7 -";

    @ClassRule
    public static MiniClusterWithClientResource cluster =
        new MiniClusterWithClientResource(new MiniClusterResourceConfiguration.Builder()
            .setNumberSlotsPerTaskManager(2)
            .setNumberTaskManagers(1)
            .build());

    @Rule
    public ExpectedException exception = ExpectedException.none();

    @Before
    public void setup() {
        CollectSink.clear();
    }

    @After
    public void tearDown() {
    }

    @Test
    public void testNext() throws Exception {
        Pattern<String, ?> pattern = Pattern.<String>begin("1").where(new Condition("a"))
            .next("2").where(new Condition("b"));
        Helper.match(TEST_STRING, pattern);
        CollectSink.assertOutput("a1 b1");
    }

    @Test
    public void testFollowedBy() throws Exception {
        Pattern<String, ?> pattern = Pattern.<String>begin("1").where(new Condition("a"))
            .followedBy("2").where(new Condition("b"));
        Helper.match(TEST_STRING, pattern);
        CollectSink.assertOutput("a1 b1", "a2 b3", "a3 b4");
    }

    @Test
    public void testFollowedBy1() throws Exception {
        Pattern<String, ?> pattern = Pattern.<String>begin("1").where(new Condition("a"))
            .followedBy("2").where(new Condition("b"))
            .followedBy("3").where(new Condition("a"));
        Helper.match(TEST_STRING, pattern);
        CollectSink.assertOutput("a1 b1 a2", "a2 b3 a3");
    }

    @Test
    public void testFollowedByAny() throws Exception {
        Pattern<String, ?> pattern = Pattern.<String>begin("1").where(new Condition("a"))
            .followedByAny("2").where(new Condition("b"));
        Helper.match(TEST_STRING, pattern);
        CollectSink.assertOutput(
            "a1 b1",
            "a1 b2",
            "a1 b3", "a2 b3",
            "a1 b4", "a2 b4", "a3 b4",
            "a1 b5", "a2 b5", "a3 b5",
            "a1 b6", "a2 b6", "a3 b6",
            "a1 b7", "a2 b7", "a3 b7"
        );
    }

    @Test
    public void testNotNext() throws Exception {
        Pattern<String, ?> pattern = Pattern.<String>begin("1").where(new Condition("a"))
            .notNext("2").where(new Condition("b"));
        Helper.match(TEST_STRING, pattern);
        CollectSink.assertOutput("a2", "a3");
    }

    @Test
    public void testNotNext2() throws Exception {
        Pattern<String, ?> pattern = Pattern.<String>begin("1").where(new Condition("a"))
            .notNext("2").where(new Condition("b"))
            .next("3").where(new Condition("a")).optional();
        Helper.match(TEST_STRING, pattern);
        // Really Problematic
        CollectSink.assertOutput("a1", "a2", "a3");
    }

    @Test
    public void testNotFollowedBy1() throws Exception {
        exception.expect(MalformedPatternException.class);
        exception.expectMessage("NotFollowedBy is not supported as a last part of a Pattern!");
        Pattern<String, ?> pattern = Pattern.<String>begin("1").where(new Condition("a"))
            .notFollowedBy("2").where(new Condition("b"));
        Helper.match(TEST_STRING, pattern);
        CollectSink.assertOutput();
    }

    @Test
    public void testNotFollowedBy2() throws Exception {
        exception.expect(MalformedPatternException.class);
        exception.expectMessage("The until condition is only applicable to looping states.");
        Pattern<String, ?> pattern = Pattern.<String>begin("1").where(new Condition("a"))
            .notFollowedBy("2").where(new Condition("b"))
            .until(new Condition("c"));
        Helper.match(TEST_STRING, pattern);
        CollectSink.assertOutput();
    }

    @Test
    public void testNotFollowedBy3() throws Exception {
        Pattern<String, ?> pattern = Pattern.<String>begin("1").where(new Condition("a"))
            .notFollowedBy("2").where(new Condition("b"))
            .followedBy("c").where(new Condition("c"));
        Helper.match(TEST_STRING, pattern);
        CollectSink.assertOutput("a3 c2");
    }

    @Test
    public void testNextOneOrMore() throws Exception {
        Pattern<String, ?> pattern = Pattern.<String>begin("1").where(new Condition("a"))
            .next("2").where(new Condition("b")).oneOrMore();
        Helper.match(TEST_STRING, pattern);
        CollectSink.assertOutput(
            "a1 b1",
            "a1 b1 b2",
            "a1 b1 b2 b3",
            "a1 b1 b2 b3 b4",
            "a1 b1 b2 b3 b4 b5",
            "a1 b1 b2 b3 b4 b5 b6",
            "a1 b1 b2 b3 b4 b5 b6 b7"
        );
    }

    @Test
    public void testNextOneOrMoreConsecutive() throws Exception {
        Pattern<String, ?> pattern = Pattern.<String>begin("1").where(new Condition("a"))
            .next("2").where(new Condition("b")).oneOrMore().consecutive();
        Helper.match(TEST_STRING, pattern);
        CollectSink.assertOutput("a1 b1", "a1 b1 b2");
    }

    @Test
    public void testFollowedByTimes() throws Exception {
        Pattern<String, ?> pattern = Pattern.<String>begin("1").where(new Condition("a"))
            .followedBy("2").where(new Condition("b")).times(2, 4);
        Helper.match(TEST_STRING, pattern);
        CollectSink.assertOutput(
            "a1 b1 b2",
            "a1 b1 b2 b3",
            "a1 b1 b2 b3 b4", "a2 b3 b4",
            "a2 b3 b4 b5", "a3 b4 b5",
            "a2 b3 b4 b5 b6", "a3 b4 b5 b6",
            "a3 b4 b5 b6 b7"
        );
    }

    @Test
    public void testFollowedByTimesGreedy() throws Exception {
        Pattern<String, ?> pattern = Pattern.<String>begin("1").where(new Condition("a"))
            .followedBy("2").where(new Condition("b")).times(2, 4).greedy();
        Helper.match(TEST_STRING, pattern);
        CollectSink.assertOutput(
            "a1 b1 b2",
            "a1 b1 b2 b3",
            "a1 b1 b2 b3 b4", "a2 b3 b4",
            "a2 b3 b4 b5", "a3 b4 b5",
            "a2 b3 b4 b5 b6", "a3 b4 b5 b6",
            "a3 b4 b5 b6 b7"
        );
    }

    @Test
    public void testFollowedByOneOrMoreConsecutive() throws Exception {
        Pattern<String, ?> pattern = Pattern.<String>begin("1").where(new Condition("a"))
            .followedBy("2").where(new Condition("b")).oneOrMore().consecutive();
        Helper.match(TEST_STRING, pattern);
        CollectSink.assertOutput(
            "a1 b1", "a1 b1 b2",
            "a2 b3",
            "a3 b4", "a3 b4 b5"
        );
    }

    @Test
    public void testFollowedByOneOrMoreUntil() throws Exception {
        Pattern<String, ?> pattern = Pattern.<String>begin("1").where(new Condition("a"))
            .followedBy("2").where(new Condition("b")).oneOrMore()
            .until(new Condition("c"));
        Helper.match(TEST_STRING, pattern);
        CollectSink.assertOutput(
            "a1 b1", "a1 b1 b2", "a1 b1 b2 b3",
            "a2 b3",
            "a3 b4", "a3 b4 b5", "a3 b4 b5 b6"
        );
    }

    @Test
    public void testFollowedByOneOrMoreGreedyUntil() throws Exception {
        Pattern<String, ?> pattern = Pattern.<String>begin("1").where(new Condition("a"))
            .followedBy("2").where(new Condition("b")).oneOrMore().greedy()
            .until(new Condition("c"));
        Helper.match(TEST_STRING, pattern);
        CollectSink.assertOutput(
            "a1 b1", "a1 b1 b2", "a1 b1 b2 b3",
            "a2 b3",
            "a3 b4", "a3 b4 b5", "a3 b4 b5 b6"
        );
    }

    @Test
    public void testFollowedByOneOrMoreUntilSkip() throws Exception {
        Pattern<String, ?> pattern = Pattern.<String>begin(
            "1",
            AfterMatchSkipStrategy.skipPastLastEvent()
        ).where(new Condition("a"))
            .followedBy("2").where(new Condition("b")).oneOrMore()
            .until(new Condition("c"));
        Helper.match(TEST_STRING, pattern);
        CollectSink.assertOutput("a1 b1", "a2 b3", "a3 b4");
    }

    @Test
    public void testGrouping() throws Exception {
        Pattern<String, ?> pattern = Pattern.<String>begin("1").where(new Condition("a"))
            .followedBy("2").where(new Condition("b")).times(2);
        Pattern<String, ?> pattern1 = Pattern.begin(pattern).times(2);
        Helper.match(TEST_STRING, pattern1);
        CollectSink.assertOutput("a1 a2 b1 b2 b3 b4");
    }
}
