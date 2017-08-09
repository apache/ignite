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

package org.apache.ignite.internal.processors.query.h2.twostep;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.processors.query.h2.twostep.msg.GridH2ValueMessageFactory;
import org.h2.result.ResultTarget;
import org.h2.value.Value;

import java.util.concurrent.Semaphore;

/**
 * Map query streaming result target.
 */
public class MapQueryStreamingResultTarget implements ResultTarget {
    /** Page size. */
    private final int pageSize;

    /** Local node flag. */
    private final boolean loc;

    /** Semaphore for blocking. */
    private final Semaphore sem = new Semaphore(1);

    /** Row counter. */
    private int rowCnt;

    /** Data. */
    private Object[] data;

    /** Cancellation flag. */
    private volatile boolean cancelled;

    /**
     * Constructor.
     *
     * @param pageSize Page size.
     * @param loc Local node flag.
     */
    public MapQueryStreamingResultTarget(int pageSize, boolean loc) {
        this.pageSize = pageSize;
        this.loc = loc;
    }

    /** {@inheritDoc} */
    @Override public void addRow(Value[] vals) {
        if (cancelled)
            return;

        int colCnt = vals.length;

        if (data == null)
            data = new Object[colCnt * pageSize];

        int idx = rowCnt * colCnt;

        if (loc) {
            for (Value val : vals)
                data[idx++] = val;
        }
        else {
            try {
                for (Value val : vals)
                    data[idx++] = GridH2ValueMessageFactory.toMessage(val);
            }
            catch (IgniteCheckedException e) {
                throw new IgniteException("Failed to convert H2 value to message.", e);
            }
        }

        rowCnt++;

        if (rowCnt == pageSize) {
            send();

            block();
        }
    }

    private void send() {
        if (!cancelled) {
            // TODO
        }
    }

    /**
     * Block data output.
     */
    private void block() {
        try {
            sem.acquire();
        }
        catch (InterruptedException e) {

        }
    }

    /**
     * Handle next page request.
     */
    public void onNextPageRequest() {
        // TODO

        sem.release();
    }

    /**
     * Cancel target (node is stopping).
     */
    public void cancel() {
        cancelled = true;

        sem.release();
    }

    /** {@inheritDoc} */
    @Override public int getRowCount() {
        return rowCnt;
    }
}
