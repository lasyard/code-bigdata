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

package io.github.lasyard.bigdata.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

public final class SparkApp {
    private SparkApp() {
    }

    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setAppName("SparkApp");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        List<Integer> integers = Arrays.asList(1, 7, 2, 8, 5, 3, 6);
        JavaRDD<Integer> rdd = sc.parallelize(integers);
        List<Integer> results = rdd
            .filter(x -> x % 2 == 0)
            .sortBy(x -> x, true, 0)
            .map(x -> x / 2)
            .collect();
        System.out.println(results);
    }
}
