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

package org.apache.ignite.internal.processors.cache;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.util.GridLongList;
import org.jetbrains.annotations.Nullable;

/**
 * Partition update counter.
 *
 * TODO FIXME consider rolling bit set implementation.
 * TODO describe ITEM structure
 * TODO add debugging info
 * TODO non-blocking version ? BitSets instead of TreeSet ?
 * TODO cleanup and comment interface
 */
public interface PartitionUpdateCounter {
    public void init(long initUpdCntr, @Nullable byte[] rawData);

    public long initial();

    public long get();

    public long next();

    public long next(long delta);

    public long reserved();

    /**
     * @param val Value.
     *
     * @throws Exception if counter cannot be set to passed value due to incompatibility with current state.
     */
    public void update(long val) throws IgniteCheckedException;

    public boolean update(long start, long delta);

    public void resetCounters();

    public void updateInitial(long cntr);

    public GridLongList finalizeUpdateCounters();

    public long reserve(long delta);

    public @Nullable byte[] getBytes();

    public boolean sequential();
}
