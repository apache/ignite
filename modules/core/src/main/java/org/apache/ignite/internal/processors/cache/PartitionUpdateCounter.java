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
 * TODO add update order tracking capabilities ?
 * TODO non-blocking version ? BitSets instead of TreeSet ?
 */
public interface PartitionUpdateCounter {
    /** */
    public static final PartitionUpdateCounter EMPTY = new PartitionUpdateCounter() {
        @Override public void init(long initUpdCntr, @Nullable byte[] rawData) {
            throw new UnsupportedOperationException();
        }

        @Override public long initial() {
            return 0;
        }

        @Override public long get() {
            return 0;
        }

        @Override public long next() {
            return 0;
        }

        @Override public long reserved() {
            return 0;
        }

        @Override public void update(long val) throws IgniteCheckedException {
            throw new UnsupportedOperationException();
        }

        @Override public boolean update(long start, long delta) {
            throw new UnsupportedOperationException();
        }

        @Override public void resetCounters() {
            throw new UnsupportedOperationException();
        }

        @Override public void updateInitial(long cntr) {
            throw new UnsupportedOperationException();
        }

        @Override public GridLongList finalizeUpdateCounters() {
            throw new UnsupportedOperationException();
        }

        @Override public long reserve(long delta) {
            throw new UnsupportedOperationException();
        }

        @Nullable @Override public byte[] getBytes() {
            throw new UnsupportedOperationException();
        }

        @Override public boolean sequential() {
            return false;
        }
    };

    public void init(long initUpdCntr, @Nullable byte[] rawData);

    public long initial();

    public long get();

    public long next();

    public long reserved();

    public void update(long val) throws IgniteCheckedException;

    public boolean update(long start, long delta);

    public void resetCounters();

    public void updateInitial(long cntr);

    public GridLongList finalizeUpdateCounters();

    public long reserve(long delta);

    public @Nullable byte[] getBytes();

    public boolean sequential();
}
