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

package io.github.lasyard.bigdata.kafka.camel;

import java.util.ResourceBundle;

public final class Constants {
    private static final ResourceBundle CONF = ResourceBundle.getBundle("test-camel-kafka");

    static final String KAFKA_URI = CONF.getString("kafka.uri");
    static final String WRITE_URI = CONF.getString("kafka.write.uri");
    static final String READ_URI = CONF.getString("kafka.read.uri");
    static final String TEST_STRING = CONF.getString("test.string");

    private Constants() {
    }
}
