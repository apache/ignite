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

package org.gridgain.grid.cache;

import javax.cache.management.*;
import org.apache.ignite.*;

/**
 * Cache metrics used to obtain statistics on cache itself.
 * Use {@link IgniteCache#metrics()} to obtain metrics for a cache.
 */
public interface CacheMetricsMxBean extends CacheStatisticsMXBean {
    /**
     * The mean time to execute tx commit.
     *
     * @return the time in µs
     */
    public float getAverageTxCommitTime();

    /**
     * The mean time to execute tx rollbacks.
     *
     * @return Number of transaction rollbacks.
     */
    public float getAverageTxRollbackTime();


    /**
     * Gets total number of transaction commits.
     *
     * @return Number of transaction commits.
     */
    public int getCacheTxCommits();

    /**
     * Gets total number of transaction rollbacks.
     *
     * @return Number of transaction rollbacks.
     */
    public int getCacheTxRollbacks();
}
