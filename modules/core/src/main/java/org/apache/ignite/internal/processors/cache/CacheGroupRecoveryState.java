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

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Arrays;
import java.util.Collections;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionTopology;
import org.apache.ignite.internal.util.typedef.internal.U;

/** */
public class CacheGroupRecoveryState implements Externalizable {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private Set<Integer> lostParts;

    /** */
    private Set<Integer> zeroParts;

    /** */
    public CacheGroupRecoveryState() {
        // No-op.
    }

    /** */
    public CacheGroupRecoveryState(CacheGroupContext grp) {
        GridDhtPartitionTopology top = grp.topology();

        lostParts = top.lostPartitions();
        zeroParts = top.fullUpdateCounters().zeroUpdateCounterPartitions();
    }

    /** */
    public Set<Integer> zeroUpdateCounterParititons() {
        return Collections.unmodifiableSet(zeroParts);
    }

    /** */
    public Set<Integer> lostParititons() {
        return Collections.unmodifiableSet(lostParts);
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        U.writeIntArray(out, lostParts.stream().mapToInt(Number::intValue).toArray());
        U.writeIntArray(out, zeroParts.stream().mapToInt(Number::intValue).toArray());
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        int[] lostParts = U.readIntArray(in);
        this.lostParts = lostParts.length == 0 ? Collections.emptySet() : Arrays.stream(lostParts).boxed().collect(Collectors.toSet());

        int[] zeroParts = U.readIntArray(in);
        this.zeroParts = zeroParts.length == 0 ? Collections.emptySet() : Arrays.stream(zeroParts).boxed().collect(Collectors.toSet());
    }
}
