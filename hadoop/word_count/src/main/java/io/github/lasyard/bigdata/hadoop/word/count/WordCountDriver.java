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

package io.github.lasyard.bigdata.hadoop.word.count;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.io.InputStream;
import java.security.PrivilegedExceptionAction;

public class WordCountDriver extends Configured implements Tool {
    private static final String INPUT_FILE = "test.txt";
    private static final String OUTPUT_DIR = "test-output";

    public static void main(String[] args) throws Exception {
        UserGroupInformation ugi = UserGroupInformation.createRemoteUser("root");
        System.exit(ugi.doAs((PrivilegedExceptionAction<Integer>) () -> {
            System.setProperty("hadoop.home.dir", "/");
            Configuration configuration = new Configuration();
            configuration.addResource("server.xml");
            FileSystem fs = FileSystem.get(configuration);
            fs.delete(new Path(OUTPUT_DIR), true);
            // Copy input file
            FSDataOutputStream out = fs.create(new Path(INPUT_FILE), true);
            InputStream in = WordCountDriver.class.getResourceAsStream("/" + INPUT_FILE);
            IOUtils.copyBytes(in, out.getWrappedStream(), 256);
            in.close();
            out.close();
            final int exitCode = ToolRunner.run(
                configuration,
                new WordCountDriver(),
                new String[]{INPUT_FILE, OUTPUT_DIR}
            );
            fs.delete(new Path(INPUT_FILE), true);
            FSDataInputStream res = fs.open(new Path(OUTPUT_DIR + "/part-r-00000"));
            IOUtils.copyBytes(res, System.out, 256);
            fs.delete(new Path(OUTPUT_DIR), true);
            return exitCode;
        }));
    }

    @Override
    public int run(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        if (args.length < 2) {
            ToolRunner.printGenericCommandUsage(System.err);
            return -1;
        }
        Job job = Job.getInstance(getConf());
        job.setJobName("WordCount");
        job.setJarByClass(WordCountDriver.class);
        job.setMapperClass(WordCountMapper.class);
        job.setCombinerClass(WordCountReducer.class);
        job.setReducerClass(WordCountReducer.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        return job.waitForCompletion(true) ? 0 : 1;
    }
}
