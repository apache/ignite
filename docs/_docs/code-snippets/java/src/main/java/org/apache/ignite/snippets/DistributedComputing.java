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
package org.apache.ignite.snippets;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCluster;
import org.apache.ignite.IgniteCompute;
import org.apache.ignite.Ignition;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.resources.IgniteInstanceResource;

public class DistributedComputing {

    void getCompute() {
        // tag::get-compute[]
        Ignite ignite = Ignition.start();

        IgniteCompute compute = ignite.compute();
        // end::get-compute[]
    }

    void getComputeForNodes() {

        // tag::get-compute-for-nodes[]
        Ignite ignite = Ignition.start();

        IgniteCompute compute = ignite.compute(ignite.cluster().forRemotes());

        // end::get-compute-for-nodes[]
    }

    void taskTimeout() {
        Ignite ignite = Ignition.start();

        // tag::timeout[]
        IgniteCompute compute = ignite.compute();

        compute.withTimeout(300_000).run(() -> {
            // your computation
            // ...
        });
        // end::timeout[]
    }

    void executeRunnable(Ignite ignite) {
        // tag::execute-runnable[]
        IgniteCompute compute = ignite.compute();

        // Iterate through all words and print
        // each word on a different cluster node.
        for (String word : "Print words on different cluster nodes".split(" ")) {
            compute.run(() -> System.out.println(word));
        }
        // end::execute-runnable[]
    }

    void executeCallable(Ignite ignite) {
        // tag::execute-callable[]
        Collection<IgniteCallable<Integer>> calls = new ArrayList<>();

        // Iterate through all words in the sentence and create callable jobs.
        for (String word : "How many characters".split(" "))
            calls.add(word::length);

        // Execute the collection of callables on the cluster.
        Collection<Integer> res = ignite.compute().call(calls);

        // Add all the word lengths received from cluster nodes.
        int total = res.stream().mapToInt(Integer::intValue).sum();
        // end::execute-callable[]
    }

    void executeIgniteClosure(Ignite ignite) {
        // tag::execute-closure[]
        IgniteCompute compute = ignite.compute();

        // Execute closure on all cluster nodes.
        Collection<Integer> res = compute.apply(String::length, Arrays.asList("How many characters".split(" ")));

        // Add all the word lengths received from cluster nodes.
        int total = res.stream().mapToInt(Integer::intValue).sum();
        // end::execute-closure[]
    }

    void broadcast(Ignite ignite) {

        // tag::broadcast[]
        // Limit broadcast to remote nodes only.
        IgniteCompute compute = ignite.compute(ignite.cluster().forRemotes());

        // Print out hello message on remote nodes in the cluster group.
        compute.broadcast(() -> System.out.println("Hello Node: " + ignite.cluster().localNode().id()));
        // end::broadcast[]
    }

    void async(Ignite ignite) {

        // tag::async[]

        IgniteCompute compute = ignite.compute();

        Collection<IgniteCallable<Integer>> calls = new ArrayList<>();

        // Iterate through all words in the sentence and create callable jobs.
        for (String word : "Count characters using a callable".split(" "))
            calls.add(word::length);

        IgniteFuture<Collection<Integer>> future = compute.callAsync(calls);

        future.listen(fut -> {
            // Total number of characters.
            int total = fut.get().stream().mapToInt(Integer::intValue).sum();

            System.out.println("Total number of characters: " + total);
        });

        // end::async[]
    }

    void shareState(Ignite ignite) {
        // tag::get-map[]
        IgniteCluster cluster = ignite.cluster();

        ConcurrentMap<String, Integer> nodeLocalMap = cluster.nodeLocalMap();
        // end::get-map[]

        // tag::job-counter[]
        IgniteCallable<Long> job = new IgniteCallable<Long>() {
            @IgniteInstanceResource
            private Ignite ignite;

            @Override
            public Long call() {
                // Get a reference to node local.
                ConcurrentMap<String, AtomicLong> nodeLocalMap = ignite.cluster().nodeLocalMap();

                AtomicLong cntr = nodeLocalMap.get("counter");

                if (cntr == null) {
                    AtomicLong old = nodeLocalMap.putIfAbsent("counter", cntr = new AtomicLong());

                    if (old != null)
                        cntr = old;
                }

                return cntr.incrementAndGet();
            }
        };

        // end::job-counter[]
    }

    // tag::access-data[]
    public class MyCallableTask implements IgniteCallable<Integer> {

        @IgniteInstanceResource
        private Ignite ignite;

        @Override
        public Integer call() throws Exception {

            IgniteCache<Long, Person> cache = ignite.cache("person");

            // Get the data you need
            Person person = cache.get(1L);

            // do with the data what you need to do

            return 1;
        }
    }

    // end::access-data[]

}
