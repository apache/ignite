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

import java.util.concurrent.ExecutorService;

import org.apache.ignite.Ignite;
import org.apache.ignite.cluster.ClusterGroup;
import org.apache.ignite.lang.IgniteRunnable;

public class IgniteExecutorService {

    void test(Ignite ignite) {

        // tag::execute[]
        // Get cluster-enabled executor service.
        ExecutorService exec = ignite.executorService();

        // Iterate through all words in the sentence and create jobs.
        for (final String word : "Print words using runnable".split(" ")) {
            // Execute runnable on some node.
            exec.submit(new IgniteRunnable() {
                @Override
                public void run() {
                    System.out.println(">>> Printing '" + word + "' on this node from grid job.");
                }
            });
        }
        // end::execute[]
    }

    void clusterGroup(Ignite ignite) {
        // tag::cluster-group[]
        // A group for nodes where the attribute 'worker' is defined.
        ClusterGroup workerGrp = ignite.cluster().forAttribute("ROLE", "worker");

        // Get an executor service for the cluster group.
        ExecutorService exec = ignite.executorService(workerGrp);
        // end::cluster-group[]

    }
}
