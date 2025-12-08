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
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.CachePartitionPartialCountersMap;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.marshaller.Marshaller;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.jetbrains.annotations.Nullable;

/**
 *
 */
public class ContinuousRoutineStartResultMessage implements Message {
    /** */
    @Order(0)
    private UUID routineId;

    /** */
    private @Nullable Exception err;

    /** */
    @Order(value = 1, method = "errorBytes")
    private byte[] errBytes;

    /** */
    @Order(value = 2, method = "countersMap")
    private CachePartitionPartialCountersMap cntrsMap;

    /**
     *
     */
    public ContinuousRoutineStartResultMessage() {
        // No-op.
    }

    /**
     * @param routineId Routine ID.
     * @param cntrsMap Counters map.
     * @param err Error.
     */
    ContinuousRoutineStartResultMessage(
        UUID routineId,
        @Nullable CachePartitionPartialCountersMap cntrsMap,
        @Nullable Exception err
    ) {
        this.routineId = routineId;
        this.cntrsMap = cntrsMap;
        this.err = err;
    }

    /**
     * @return Counters map.
     */
    public @Nullable CachePartitionPartialCountersMap countersMap() {
        return cntrsMap;
    }

    /**
     * @param cntrsMap Counters map.
     */
    public void countersMap(@Nullable CachePartitionPartialCountersMap cntrsMap) {
        this.cntrsMap = cntrsMap;
    }

    /**
     * @return {@code True} if failed to start routine.
     */
    boolean hasError() {
        return err != null || errBytes != null;
    }

    /**
     * @return Routine ID.
     */
    public UUID routineId() {
        return routineId;
    }

    /**
     * @param routineId Routine ID.
     */
    public void routineId(UUID routineId) {
        this.routineId = routineId;
    }

    /**
     * @return Error bytes.
     */
    public @Nullable byte[] errorBytes() {
        return errBytes;
    }

    /**
     * @param errBytes Error bytes.
     */
    public void errorBytes(@Nullable byte[] errBytes) {
        this.errBytes = errBytes;
    }

    /**
     * @return Error.
     */
    public @Nullable Exception error() {
        return err;
    }

    /**
     * Marshal error to byte array.
     *
     * @param marsh Marshaller.
     */
    public void marshalError(Marshaller marsh) throws IgniteCheckedException {
        if (err != null)
            errBytes = U.marshal(marsh, err);
    }

    /**
     * Unmarshal error from byte array.
     *
     * @param marsh Marshaller.
     * @param ldr Class loader.
     */
    public void unmarshalError(Marshaller marsh, ClassLoader ldr) throws IgniteCheckedException {
        if (errBytes != null && err == null) {
            err = U.unmarshal(marsh, errBytes, ldr);

            errBytes = null;
        }
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return 134;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(ContinuousRoutineStartResultMessage.class, this);
    }
}
