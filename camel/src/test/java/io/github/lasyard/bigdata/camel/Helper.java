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
import org.apache.camel.Exchange;
import org.apache.camel.test.junit4.CamelTestSupport;

import java.io.File;
import java.util.ResourceBundle;
import javax.annotation.Nonnull;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

@Slf4j
final class Helper {
    private static final ResourceBundle res = ResourceBundle.getBundle("test-camel-file");

    private static final String testFileName = res.getString("test.file.name");
    private static final String inputDir = res.getString("file.inbox");
    static final String fileInputUri = "file:" + inputDir + "?noop=true";
    private static final String outputDir = res.getString("file.outbox");
    static final String fileOutputUri = "file:" + outputDir;

    private Helper() {
    }

    static void cleanUp() {
        CamelTestSupport.deleteDirectory(inputDir);
        log.info(inputDir + " deleted.");
        CamelTestSupport.deleteDirectory(outputDir);
        log.info(outputDir + " deleted.");
    }

    static void testCopyFile(@Nonnull CamelTestSupport cts) throws Exception {
        log.debug("fileInputUri = {}.", fileInputUri);
        cts.context().getRouteController().startAllRoutes();
        String testString = Double.toString(Math.random());
        cts.template().sendBodyAndHeader(fileInputUri, testString, Exchange.FILE_NAME, testFileName);
        File srcFile = new File(inputDir + File.separator + testFileName);
        assertTrue(srcFile.exists());
        // By default, camel checks the directory twice a second.
        Thread.sleep(3000);
        File dstFile = new File(outputDir + File.separator + testFileName);
        assertTrue(dstFile.exists());
        String content = cts.context().getTypeConverter().convertTo(String.class, dstFile);
        assertThat(content, is(testString));
        cts.context().stop();
    }
}
