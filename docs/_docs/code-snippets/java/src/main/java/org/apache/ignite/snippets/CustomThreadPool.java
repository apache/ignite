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

import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.ExecutorConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.lang.IgniteRunnable;
import org.apache.ignite.resources.IgniteInstanceResource;

public class CustomThreadPool {

    void customPool() {

        // tag::pool-config[]
        IgniteConfiguration cfg = new IgniteConfiguration();

        cfg.setExecutorConfiguration(new ExecutorConfiguration("myPool").setSize(16));
        // end::pool-config[]

        Ignite ignite = Ignition.start(cfg);

        ignite.compute().run(new OuterRunnable());

    }

    // tag::inner-runnable[]
    public class InnerRunnable implements IgniteRunnable {
        @Override
        public void run() {
            System.out.println("Hello from inner runnable!");
        }
    }
    // end::inner-runnable[]

    // tag::outer-runnable[]
    public class OuterRunnable implements IgniteRunnable {
        @IgniteInstanceResource
        private Ignite ignite;

        @Override
        public void run() {
            // Synchronously execute InnerRunnable in a custom executor.
            ignite.compute().withExecutor("myPool").run(new InnerRunnable());
            System.out.println("outer runnable is executed");
        }
    }
    // end::outer-runnable[]

    public static void main(String[] args) {
        CustomThreadPool ctp = new CustomThreadPool();
        ctp.customPool();
    }
}
