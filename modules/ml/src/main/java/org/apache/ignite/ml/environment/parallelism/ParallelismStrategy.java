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

package org.apache.ignite.ml.environment.parallelism;

import java.util.ArrayList;
import java.util.List;
import org.apache.ignite.ml.math.functions.IgniteSupplier;

/**
 * Specifies the behaviour of processes in ML-algorithms that can may be parallelized such as parallel learning in
 * bagging, learning submodels for One-vs-All model, Cross-Validation etc.
 */
public interface ParallelismStrategy {
    public enum Type {
        NO_PARALLELISM,
        ON_DEFAULT_POOL
    }

    /**
     * Submit task.
     *
     * @param task Task.
     */
    public <T> Promise<T> submit(IgniteSupplier<T> task);

    public default <T> List<Promise<T>> submit(List<IgniteSupplier<T>> tasks) {
        List<Promise<T>> results = new ArrayList<>();
        for(IgniteSupplier<T> task : tasks)
            results.add(submit(task));
        return results;
    }
}
