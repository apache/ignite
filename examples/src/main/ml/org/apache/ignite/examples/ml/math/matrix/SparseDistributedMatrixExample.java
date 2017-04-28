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

package org.apache.ignite.examples.ml.math.matrix;

import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.ml.math.Matrix;
import org.apache.ignite.ml.math.StorageConstants;
import org.apache.ignite.ml.math.impls.matrix.CacheMatrix;
import org.apache.ignite.ml.math.impls.matrix.SparseDistributedMatrix;
import org.apache.ignite.thread.IgniteThread;

/**
 * This example shows how to create and use {@link SparseDistributedMatrix} API.
 *
 * Unlike the {@link CacheMatrix} the {@link SparseDistributedMatrix} creates it's own cache.
 */
public class SparseDistributedMatrixExample {
    /**
     * Executes example.
     *
     * @param args Command line arguments, none required.
     */
    public static void main(String[] args) throws InterruptedException {
        System.out.println();
        System.out.println(">>> Sparse distributed matrix API usage example started.");
        // Start ignite grid.
        try (Ignite ignite = Ignition.start("examples/config/example-ignite.xml")) {
            System.out.println(">>> Ignite grid started.");
            // Create IgniteThread, we must work with SparseDistributedMatrix inside IgniteThread
            // because we create ignite cache internally.
            IgniteThread igniteThread = new IgniteThread(ignite.configuration().getIgniteInstanceName(), SparseDistributedMatrixExample.class.getSimpleName(), () -> {

                double[][] testValues = {{1.0, 0.0, 0.0}, {0.0, 1.0, 0.0}, {0.0, 0.0, 1.0}};

                System.out.println(">>> Create new SparseDistributedMatrix inside IgniteThread.");
                // Create SparseDistributedMatrix, new cache will be created automagically.
                SparseDistributedMatrix distributedMatrix = new SparseDistributedMatrix(testValues.length, testValues[0].length,
                    StorageConstants.ROW_STORAGE_MODE, StorageConstants.RANDOM_ACCESS_MODE);

                distributedMatrix.assign(testValues);

                System.out.println("Sum of all matrix elements is " + distributedMatrix.sum());

                System.out.println(">>> Destroy SparseDistributedMatrix after using.");
                // Destroy internal cache.
                distributedMatrix.destroy();
            });

            igniteThread.start();

            igniteThread.join();
        }
    }
}
