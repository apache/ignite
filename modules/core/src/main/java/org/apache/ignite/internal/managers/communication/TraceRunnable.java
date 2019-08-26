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

package org.apache.ignite.internal.managers.communication;

import org.apache.ignite.internal.processors.tracing.MTC;
import org.apache.ignite.internal.processors.tracing.MTC.TraceSurroundings;
import org.apache.ignite.internal.processors.tracing.Span;
import org.apache.ignite.internal.processors.tracing.Tracing;

/**
 * Wrapper of {@link Runnable} which incject tracing to execution.
 */
public abstract class TraceRunnable implements Runnable {
    /** */
    private final Tracing tracing;

    /** Name of new span. */
    private final String processName;

    /** Parent span from which new span should be created. */
    private final Span parent;

    /**
     * @param tracing Tracing processor.
     * @param name Name of new span.
     */
    public TraceRunnable(Tracing tracing, String name) {
        this.tracing = tracing;
        processName = name;
        this.parent = MTC.span();
    }

    /** {@inheritDoc} */
    @Override public void run() {
        try (TraceSurroundings ignore = tracing.startChild(processName, parent)) {
            execute();
        }
    }

    /**
     * Main code to execution.
     */
    public abstract void execute();
}
