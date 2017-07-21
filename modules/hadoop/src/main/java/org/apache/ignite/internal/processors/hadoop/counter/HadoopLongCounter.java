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

package org.apache.ignite.internal.processors.hadoop.counter;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * Standard hadoop counter to use via original Hadoop API in Hadoop jobs.
 */
public class HadoopLongCounter extends HadoopCounterAdapter {
    /** */
    private static final long serialVersionUID = 0L;

    /** The counter value. */
    private long val;

    /**
     * Default constructor required by {@link Externalizable}.
     */
    public HadoopLongCounter() {
        // No-op.
    }

    /**
     * Constructor.
     *
     * @param grp Group name.
     * @param name Counter name.
     */
    public HadoopLongCounter(String grp, String name) {
        super(grp, name);
    }

    /** {@inheritDoc} */
    @Override protected void writeValue(ObjectOutput out) throws IOException {
        out.writeLong(val);
    }

    /** {@inheritDoc} */
    @Override protected void readValue(ObjectInput in) throws IOException {
        val = in.readLong();
    }

    /** {@inheritDoc} */
    @Override public void merge(HadoopCounter cntr) {
        val += ((HadoopLongCounter)cntr).val;
    }

    /**
     * Gets current value of this counter.
     *
     * @return Current value.
     */
    public long value() {
        return val;
    }

    /**
     * Sets current value by the given value.
     *
     * @param val Value to set.
     */
    public void value(long val) {
        this.val = val;
    }

    /**
     * Increment this counter by the given value.
     *
     * @param i Value to increase this counter by.
     */
    public void increment(long i) {
        val += i;
    }
}