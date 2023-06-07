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

package org.apache.ignite.internal.management.consistency;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import org.apache.ignite.cache.ReadRepairStrategy;
import org.apache.ignite.internal.dto.IgniteDataTransferObject;
import org.apache.ignite.internal.management.api.Argument;
import org.apache.ignite.internal.util.typedef.internal.U;

/** */
public class ConsistencyRepairCommandArg extends IgniteDataTransferObject {
    /** */
    private static final long serialVersionUID = 0;

    /** */
    @Argument(description = "Cache to be checked/repaired")
    private String cache;

    /** */
    @Argument(description = "Cache's partition to be checked/repaired", example = "partition")
    private int[] partition;

    /** Strategy. */
    @Argument(description = "Repair strategy")
    ReadRepairStrategy strategy;

    /** */
    @Argument(description = "Run concurrently on each node", optional = true)
    private boolean parallel;

    /** */
    public void ensureParams() {
        // see https://issues.apache.org/jira/browse/IGNITE-15316
        if (parallel && strategy != ReadRepairStrategy.CHECK_ONLY) {
            throw new UnsupportedOperationException(
                "Parallel mode currently allowed only when CHECK_ONLY strategy is chosen.");
        }
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        U.writeString(out, cache);
        U.writeIntArray(out, partition);
        U.writeEnum(out, strategy);
        out.writeBoolean(parallel);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(byte protoVer, ObjectInput in) throws IOException, ClassNotFoundException {
        cache = U.readString(in);
        partition = U.readIntArray(in);
        strategy = U.readEnum(in, ReadRepairStrategy.class);
        parallel = in.readBoolean();
    }

    /** */
    public int[] partition() {
        return partition;
    }

    /** */
    public void partition(int[] partition) {
        this.partition = partition;
    }

    /** */
    public String cache() {
        return cache;
    }

    /** */
    public void cache(String cacheName) {
        this.cache = cacheName;
    }

    /** */
    public ReadRepairStrategy strategy() {
        return strategy;
    }

    /** */
    public void strategy(ReadRepairStrategy strategy) {
        this.strategy = strategy;
        ensureParams();
    }

    /** */
    public boolean parallel() {
        return parallel;
    }

    /** */
    public void parallel(boolean parallel) {
        this.parallel = parallel;
        ensureParams();
    }
}
