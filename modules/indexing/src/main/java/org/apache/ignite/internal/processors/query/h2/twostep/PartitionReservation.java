/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.query.h2.twostep;

import org.apache.ignite.internal.processors.cache.distributed.dht.GridReservable;
import org.jetbrains.annotations.Nullable;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Partition reservation for specific query.
 */
public class PartitionReservation {
    /** Reserved partitions. */
    private final List<GridReservable> reserved;

    /** Error message. */
    private final String err;

    /** Release guard. */
    private final AtomicBoolean releaseGuard = new AtomicBoolean();

    /**
     * Constructor for successfull reservation.
     *
     * @param reserved Reserved partitions.
     */
    public PartitionReservation(List<GridReservable> reserved) {
        this(reserved, null);
    }

    /**
     * Base constructor.
     *
     * @param reserved Reserved partitions.
     * @param err Error message.
     */
    public  PartitionReservation(@Nullable List<GridReservable> reserved, @Nullable String err) {
        this.reserved = reserved;
        this.err = err;
    }

    /**
     * @return Error message (if any).
     */
    @Nullable public String error() {
        return err;
    }

    /**
     * @return {@code True} if reservation failed.
     */
    public boolean failed() {
        return err != null;
    }

    /**
     * Release partitions.
     */
    public void release() {
        if (!releaseGuard.compareAndSet(false, true))
            return;

        if (reserved != null) {
            for (int i = 0; i < reserved.size(); i++)
                reserved.get(i).release();
        }
    }
}
