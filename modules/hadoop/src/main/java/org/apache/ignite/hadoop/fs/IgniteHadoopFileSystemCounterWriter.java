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

package org.apache.ignite.hadoop.fs;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.processors.hadoop.HadoopJobEx;
import org.apache.ignite.internal.processors.hadoop.counter.HadoopCounterWriter;
import org.apache.ignite.internal.processors.hadoop.counter.HadoopCounters;
import org.apache.ignite.internal.processors.hadoop.delegate.HadoopDelegateUtils;
import org.apache.ignite.internal.processors.hadoop.delegate.HadoopFileSystemCounterWriterDelegate;

/**
 * Statistic writer implementation that writes info into any Hadoop file system.
 */
public class IgniteHadoopFileSystemCounterWriter implements HadoopCounterWriter {
    /** */
    public static final String PERFORMANCE_COUNTER_FILE_NAME = "performance";

    /** */
    public static final String COUNTER_WRITER_DIR_PROPERTY = "ignite.counters.fswriter.directory";

    /** Mutex. */
    private final Object mux = new Object();

    /** Delegate. */
    private volatile HadoopFileSystemCounterWriterDelegate delegate;

    /** {@inheritDoc} */
    @Override public void write(HadoopJobEx job, HadoopCounters cntrs)
        throws IgniteCheckedException {
        delegate(job).write(job, cntrs);
    }

    /**
     * Get delegate creating it if needed.
     *
     * @param job Job.
     * @return Delegate.
     */
    private HadoopFileSystemCounterWriterDelegate delegate(HadoopJobEx job) {
        HadoopFileSystemCounterWriterDelegate delegate0 = delegate;

        if (delegate0 == null) {
            synchronized (mux) {
                delegate0 = delegate;

                if (delegate0 == null) {
                    delegate0 = HadoopDelegateUtils.counterWriterDelegate(job.getClass().getClassLoader(), this);

                    delegate = delegate0;
                }
            }
        }

        return delegate0;
    }
}
