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

package org.apache.ignite.spi.tracing.opencensus;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.processors.tracing.SpanStatus;

/**
 * Table to match OpenCensus span statuses with declared in Tracing SPI.
 */
public class StatusMatchTable {
    /** Table. */
    private static final Map<SpanStatus, io.opencensus.trace.Status> table = new ConcurrentHashMap<>();

    static {
        table.put(SpanStatus.OK, io.opencensus.trace.Status.OK);
        table.put(SpanStatus.CANCELLED, io.opencensus.trace.Status.CANCELLED);
        table.put(SpanStatus.ABORTED, io.opencensus.trace.Status.ABORTED);
    }

    /**
     * Default constructor.
     */
    private StatusMatchTable() {
    }

    /**
     * @param spanStatus Span status.
     */
    public static io.opencensus.trace.Status match(SpanStatus spanStatus) {
        io.opencensus.trace.Status res = table.get(spanStatus);

        if (res == null)
            throw new IgniteException("Unknown span status (no matching with OpenCensus): " + spanStatus);

        return res;
    }
}
