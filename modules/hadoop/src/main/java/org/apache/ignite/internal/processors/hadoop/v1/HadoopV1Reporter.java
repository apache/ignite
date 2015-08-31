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

package org.apache.ignite.internal.processors.hadoop.v1;

import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.Reporter;
import org.apache.ignite.internal.processors.hadoop.HadoopTaskContext;
import org.apache.ignite.internal.processors.hadoop.counter.HadoopLongCounter;

/**
 * Hadoop reporter implementation for v1 API.
 */
public class HadoopV1Reporter implements Reporter {
    /** Context. */
    private final HadoopTaskContext ctx;

    /**
     * Creates new instance.
     *
     * @param ctx Context.
     */
    public HadoopV1Reporter(HadoopTaskContext ctx) {
        this.ctx = ctx;
    }

    /** {@inheritDoc} */
    @Override public void setStatus(String status) {
        // TODO
    }

    /** {@inheritDoc} */
    @Override public Counters.Counter getCounter(Enum<?> name) {
        return getCounter(name.getDeclaringClass().getName(), name.name());
    }

    /** {@inheritDoc} */
    @Override public Counters.Counter getCounter(String grp, String name) {
        return new HadoopV1Counter(ctx.counter(grp, name, HadoopLongCounter.class));
    }

    /** {@inheritDoc} */
    @Override public void incrCounter(Enum<?> key, long amount) {
        getCounter(key).increment(amount);
    }

    /** {@inheritDoc} */
    @Override public void incrCounter(String grp, String cntr, long amount) {
        getCounter(grp, cntr).increment(amount);
    }

    /** {@inheritDoc} */
    @Override public InputSplit getInputSplit() throws UnsupportedOperationException {
        throw new UnsupportedOperationException("reporter has no input"); // TODO
    }

    /** {@inheritDoc} */
    @Override public float getProgress() {
        return 0.5f; // TODO
    }

    /** {@inheritDoc} */
    @Override public void progress() {
        // TODO
    }
}