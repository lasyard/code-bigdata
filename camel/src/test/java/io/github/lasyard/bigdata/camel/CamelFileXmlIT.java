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

package io.github.lasyard.bigdata.camel;

import lombok.extern.slf4j.Slf4j;
import org.apache.camel.test.spring.CamelSpringTestSupport;
import org.junit.Test;
import org.springframework.context.support.AbstractXmlApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

@Slf4j
public class CamelFileXmlIT extends CamelSpringTestSupport {
    @Override
    public void setUp() throws Exception {
        super.setUp();
    }

    @Override
    public void tearDown() throws Exception {
        Helper.cleanUp();
        super.tearDown();
    }

    @Override
    public AbstractXmlApplicationContext createApplicationContext() {
        return new ClassPathXmlApplicationContext("test-camel-file.xml");
    }

    @Test
    public void copyFilesTest() throws Exception {
        Helper.testCopyFile(this);
    }
}
