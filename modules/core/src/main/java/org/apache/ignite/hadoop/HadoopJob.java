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
package org.apache.ignite.hadoop;

import java.util.Collection;
import org.jetbrains.annotations.Nullable;

/**
 * Compact job description.
 */
public interface HadoopJob {
    /**
     * Gets collection of input splits for this job.
     *
     * @return Input splits.
     */
    public Collection<HadoopInputSplit> input();

    /**
     * Gets optional configuration property for the job.
     *
     * @param name Property name.
     * @return Value or {@code null} if none.
     */
    @Nullable String property(String name);

    /**
     * Checks whether job has combiner.
     *
     * @return {@code true} If job has combiner.
     */
    boolean hasCombiner();

    /**
     * Checks whether job has reducer.
     * Actual number of reducers will be in {@link HadoopMapReducePlan#reducers()}.
     *
     * @return Number of reducer.
     */
    boolean hasReducer();

    /**
     * @return Number of reducers configured for job.
     */
    int reducers();

    /**
     * Gets job name.
     *
     * @return Job name.
     */
    String jobName();

    /**
     * Gets user name.
     *
     * @return User name.
     */
    String user();
}
