/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.ml.math.benchmark;

import org.junit.Ignore;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

/** */
public class MathBenchmarkSelfTest {
    /** */
    @Test
    @Ignore("Benchmark tests are intended only for manual execution")
    public void demoTest() throws Exception {
        for (int i = 0; i < 2; i++)
            new MathBenchmark("demo test")
                .outputToConsole() // IMPL NOTE this is to write output into console instead of a file
                .tag(null) // IMPL NOTE try null for tag, expect it to be formatted reasonably
                .comments(null) // IMPL NOTE try null for comments, expect it to be formatted reasonably
                .execute(() -> {
                    double seed = 1.1;

                    for (int cnt = 0; cnt < 1000; cnt++) {
                        seed = Math.pow(seed, 2);

                        assertTrue(seed > 0);
                    }
                });
    }

    /** */
    @Test
    @Ignore("Benchmark tests are intended only for manual execution")
    public void configTest() throws Exception {
        new MathBenchmark("demo config test")
            .outputToConsole()
            .measurementTimes(2)
            .warmUpTimes(0)
            .tag("demo tag")
            .comments("demo comments")
            .execute(() -> System.out.println("config test"));
    }

    /** */
    @Test(expected = IllegalArgumentException.class)
    @Ignore("Benchmark tests are intended only for manual execution")
    public void emptyNameTest() throws Exception {
        new MathBenchmark("")
            .outputToConsole()
            .measurementTimes(1)
            .warmUpTimes(1)
            .tag("empty name test tag")
            .comments("empty name test comments")
            .execute(() -> System.out.println("empty name test"));
    }

    /** */
    @Test(expected = IllegalArgumentException.class)
    @Ignore("Benchmark tests are intended only for manual execution")
    public void nullDropboxPathTest() throws Exception {
        new ResultsWriter(null, "whatever", "whatever");
    }

    /** */
    @Test(expected = IllegalArgumentException.class)
    @Ignore("Benchmark tests are intended only for manual execution")
    public void nullDropboxUrlTest() throws Exception {
        new ResultsWriter("whatever", null, "whatever");
    }

    /** */
    @Test(expected = IllegalArgumentException.class)
    @Ignore("Benchmark tests are intended only for manual execution")
    public void nullDropboxTokenTest() throws Exception {
        new ResultsWriter("whatever", "whatever", null);
    }

    /** */
    @Test(expected = IllegalArgumentException.class)
    @Ignore("Benchmark tests are intended only for manual execution")
    public void nullResultsTest() throws Exception {
        new ResultsWriter("whatever", "whatever", "whatever").append(null);
    }
}
