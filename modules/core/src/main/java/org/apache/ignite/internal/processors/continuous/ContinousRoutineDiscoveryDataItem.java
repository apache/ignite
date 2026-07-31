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

package org.apache.ignite.internal.processors.continuous;

import java.util.UUID;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.Marshalled;
import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.UseBinaryMarshaller;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.jetbrains.annotations.Nullable;

/** Discovery data item. */
@UseBinaryMarshaller
public class ContinousRoutineDiscoveryDataItem implements Message {
    /** */
    @Order(0)
    UUID routineId;

    /** */
    @Marshalled("prjPredBytes")
    IgnitePredicate<ClusterNode> prjPred;

    /** Marshalled {@link #prjPred}. */
    @Order(1)
    byte[] prjPredBytes;

    /** Handler. */
    @Order(2)
    GridContinuousHandler hnd;

    /** Buffer size. */
    @Order(3)
    int bufSize;

    /** Time interval. */
    @Order(4)
    long interval;

    /** Automatic unsubscribe flag. */
    @Order(5)
    boolean autoUnsubscribe;

    /** Empty constructor for serialization porposes. */
    public ContinousRoutineDiscoveryDataItem() {
        // No-op.
    }

    /**
     * @param routineId Consume ID.
     * @param prjPred Projection predicate.
     * @param hnd Handler.
     * @param bufSize Buffer size.
     * @param interval Time interval.
     * @param autoUnsubscribe Automatic unsubscribe flag.
     */
    ContinousRoutineDiscoveryDataItem(UUID routineId,
        @Nullable IgnitePredicate<ClusterNode> prjPred,
        GridContinuousHandler hnd,
        int bufSize,
        long interval,
        boolean autoUnsubscribe
    ) {
        assert routineId != null;
        assert hnd != null;
        assert bufSize > 0;
        assert interval >= 0;

        this.routineId = routineId;
        this.prjPred = prjPred;
        this.hnd = hnd;
        this.bufSize = bufSize;
        this.interval = interval;
        this.autoUnsubscribe = autoUnsubscribe;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(ContinousRoutineDiscoveryDataItem.class, this);
    }
}
