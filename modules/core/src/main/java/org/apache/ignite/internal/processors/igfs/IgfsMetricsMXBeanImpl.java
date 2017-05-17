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

package org.apache.ignite.internal.processors.igfs;

/**
 * IGFS metrics bean implementation.
 */
public class IgfsMetricsMXBeanImpl implements IgfsMetricsMXBean {
    /** */
    private final IgfsEx igfs;

    /**
     * Constructor.
     */
    IgfsMetricsMXBeanImpl(IgfsEx igfs) {
        assert igfs != null;

        this.igfs = igfs;
    }

    /** {@inheritDoc} */
    @Override public String fullMetrics() {
        return toMultilineFormat(igfs.metrics().toString());
    }

    /** {@inheritDoc} */
    @Override public String localMetrics() {
        IgfsContext ctx = igfs.context();

        IgfsLocalMetrics metrics = ctx.metrics();

        IgfsMetricsAdapter a = new IgfsMetricsAdapter(
            ctx.data().spaceSize(),
            ctx.data().maxSpaceSize(),
            0, // sec space size
            0, // dir cnt
            0, // files cnt
            metrics.filesOpenedForRead(),
            metrics.filesOpenedForWrite(),
            metrics.readBlocks(),
            metrics.readBlocksSecondary(),
            metrics.writeBlocks(),
            metrics.writeBlocksSecondary(),
            metrics.readBytes(),
            metrics.readBytesTime(),
            metrics.writeBytes(),
            metrics.writeBytesTime());

        return toMultilineFormat(a.toString()
            .replaceFirst("secondarySpaceSize=[0-9]+,\\s*", "")
            .replaceFirst("dirsCnt=[0-9]+,\\s*", "")
            .replaceFirst("filesCnt=[0-9]+,\\s*", ""));
    }

    /**
     * Utility method to provide multiline value formatting.
     *
     * @param s The initial String.
     * @return Well-formatted multiline String.
     */
    private String toMultilineFormat(String s) {
        int idx1 = s.indexOf('[');

        if (idx1 >= 0)
            s = s.substring(idx1 + 1);

        int idx2 = s.indexOf(']');

        if (idx2 >=0)
            s = s.substring(0, idx2);

        return s.replaceAll(",", "\n");
    }

    /** {@inheritDoc} */
    @Override public void reset() {
        igfs.resetMetrics();
    }
}
