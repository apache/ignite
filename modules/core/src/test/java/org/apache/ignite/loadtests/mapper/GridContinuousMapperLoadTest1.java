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

package org.apache.ignite.loadtests.mapper;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.X;

/**
 * Continuous mapper load test.
 */
public class GridContinuousMapperLoadTest1 {
    /**
     * Main method.
     *
     * @param args Parameters.
     */
    public static void main(String[] args) {
        try (Ignite g = G.start("examples/config/example-cache.xml")) {
            int max = 30000;

            IgniteDataStreamer<Integer, TestObject> ldr = g.dataStreamer("replicated");

            for (int i = 0; i < max; i++)
                ldr.addData(i, new TestObject(i, "Test object: " + i));

            // Wait for loader to complete.
            ldr.close(false);

            X.println("Populated replicated cache.");

            g.compute().execute(new GridContinuousMapperTask1(), max);
        }
    }
}