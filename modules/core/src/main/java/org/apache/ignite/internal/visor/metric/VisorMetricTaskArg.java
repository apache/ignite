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

package org.apache.ignite.internal.visor.metric;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import org.apache.ignite.internal.dto.IgniteDataTransferObject;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;

/** Represents argument for {@link VisorMetricTask} execution. */
public class VisorMetricTaskArg extends IgniteDataTransferObject {
    /** */
    private static final long serialVersionUID = 0L;

    /** Name of a particular metric or metric registry. */
    private String name;

    /**
     * New bounds of histogram metric.
     * @see org.apache.ignite.internal.processors.metric.impl.HistogramMetricImpl#bounds(long[])
     * @see org.apache.ignite.mxbean.MetricsMxBean#configureHistogramMetric(String, long[])
     */
    private long[] bounds;

    /**
     * New rate time internal of hirtate metric.
     * @see org.apache.ignite.internal.processors.metric.impl.HitRateMetric#reset(long)
     * @see org.apache.ignite.mxbean.MetricsMxBean#configureHitRateMetric(String, long)
     */
    private long rateTimeInterval;

    /** Default constructor. */
    public VisorMetricTaskArg() {
        // No-op.
    }

    /** @param name Name of a particular metric or metric registry. */
    public VisorMetricTaskArg(String name, long[] bounds, long rateTimeInterval) {
        this.name = name;
        this.bounds = bounds;
        this.rateTimeInterval = rateTimeInterval;
    }

    /** @return Name of a particular metric or metric registry. */
    public String name() {
        return name;
    }

    /** @return New bounds of histgoram metric. */
    public long[] bounds() {
        return bounds;
    }

    /** @return New rate time internal. */
    public long rateTimeInterval() {
        return rateTimeInterval;
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        U.writeString(out, name);
        U.writeLongArray(out, bounds);
        out.writeLong(rateTimeInterval);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(byte protoVer, ObjectInput in) throws IOException, ClassNotFoundException {
        name = U.readString(in);
        bounds = U.readLongArray(in);
        rateTimeInterval = in.readLong();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(VisorMetricTaskArg.class, this);
    }
}
