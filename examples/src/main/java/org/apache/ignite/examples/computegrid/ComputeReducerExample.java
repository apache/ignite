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

package org.apache.ignite.examples.computegrid;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteException;
import org.apache.ignite.Ignition;
import org.apache.ignite.examples.ExampleNodeStartup;
import org.apache.ignite.lang.IgniteClosure;
import org.apache.ignite.lang.IgniteReducer;

/**
 * Demonstrates a simple use of Ignite with reduce closure.
 * <p>
 * Phrase is split into words and distributed across nodes where length of each word is
 * calculated. Then total phrase length is calculated using reducer.
 * <p>
 * Remote nodes should always be started with special configuration file which
 * enables P2P class loading: {@code 'ignite.{sh|bat} examples/config/example-ignite.xml'}.
 * <p>
 * Alternatively you can run {@link ExampleNodeStartup} in another JVM which will start Ignite node
 * with {@code examples/config/example-ignite.xml} configuration.
 */
public class ComputeReducerExample {
    /**
     * Executes example.
     *
     * @param args Command line arguments, none required.
     * @throws IgniteException If example execution failed.
     */
    public static void main(String[] args) throws IgniteException {
        try (Ignite ignite = Ignition.start("examples/config/example-ignite.xml")) {
            System.out.println();
            System.out.println("Compute reducer example started.");

            Integer sum = ignite.compute().apply(
                new IgniteClosure<String, Integer>() {
                    @Override public Integer apply(String word) {
                        System.out.println();
                        System.out.println(">>> Printing '" + word + "' on this node from ignite job.");

                        // Return number of letters in the word.
                        return word.length();
                    }
                },

                // Job parameters. Ignite will create as many jobs as there are parameters.
                Arrays.asList("Count characters using reducer".split(" ")),

                // Reducer to process results as they come.
                new IgniteReducer<Integer, Integer>() {
                    private AtomicInteger sum = new AtomicInteger();

                    // Callback for every job result.
                    @Override public boolean collect(Integer len) {
                        sum.addAndGet(len);

                        // Return true to continue waiting until all results are received.
                        return true;
                    }

                    // Reduce all results into one.
                    @Override public Integer reduce() {
                        return sum.get();
                    }
                }
            );

            System.out.println();
            System.out.println(">>> Total number of characters in the phrase is '" + sum + "'.");
            System.out.println(">>> Check all nodes for output (this node is also part of the cluster).");
        }
    }
}