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

import org.apache.camel.test.spring.CamelSpringTestSupport;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.support.AbstractXmlApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

public class CamelKafkaXmlIT extends CamelSpringTestSupport {
    private static final Logger logger = LoggerFactory.getLogger(CamelKafkaXmlIT.class);

    @Override
    public void setUp() throws Exception {
        super.setUp();
    }

    @Override
    public void tearDown() throws Exception {
        logger.info("Test done. Tear down.");
        super.tearDown();
    }

    @Override
    public AbstractXmlApplicationContext createApplicationContext() {
        return new ClassPathXmlApplicationContext("test-camel-kafka.xml");
    }

    @Test
    public void testKafka() throws Exception {
        Helper.testKafka(this);
    }
}
