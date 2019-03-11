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

package org.apache.ignite.yardstick.probes;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.pagemem.wal.IgniteWriteAheadLogManager;
import org.apache.ignite.internal.pagemem.wal.record.MemoryRecoveryRecord;
import org.apache.ignite.internal.processors.cache.persistence.wal.FileWALPointer;
import org.apache.ignite.yardstick.IgniteAbstractBenchmark;
import org.yardstickframework.BenchmarkConfiguration;
import org.yardstickframework.BenchmarkDriver;
import org.yardstickframework.BenchmarkProbePoint;
import org.yardstickframework.BenchmarkTotalsOnlyProbe;

/**
 * WAL size probe.
 */
public class WalSizeProbe implements BenchmarkTotalsOnlyProbe {
    /** WAL manager. */
    private volatile IgniteWriteAheadLogManager wal;

    /** Wal segment size. */
    private volatile int walSegmentSize;

    private volatile FileWALPointer prevPtr;

    /** Benchmark points. */
    private final List<BenchmarkProbePoint> points = new LinkedList<>();

    /** {@inheritDoc} */
    @Override public void start(BenchmarkDriver drv, BenchmarkConfiguration cfg) throws Exception {
        if (drv instanceof IgniteAbstractBenchmark) {
            IgniteEx ignite = (IgniteEx)((IgniteAbstractBenchmark)drv).ignite();

            wal = ignite.context().cache().context().wal();

            if (wal != null) {
                walSegmentSize = ignite.configuration().getDataStorageConfiguration().getWalSegmentSize();

                prevPtr = (FileWALPointer)wal.log(new MemoryRecoveryRecord(0));
            }
        }
    }

    /** {@inheritDoc} */
    @Override public void stop() throws Exception {

    }

    /** {@inheritDoc} */
    @Override public Collection<String> metaInfo() {
        return Arrays.asList("Time, sec", "WAL segments", "Estimated WAL size");    }

    /** {@inheritDoc} */
    @Override public Collection<BenchmarkProbePoint> points() {return Collections.unmodifiableList(points);
    }

    /** {@inheritDoc} */
    @Override public void buildPoint(long time) {
        if (wal == null)
            return;

        try {
            FileWALPointer ptr = (FileWALPointer)wal.log(new MemoryRecoveryRecord(time));

            double estimatedSize = (ptr.index() - prevPtr.index()) * walSegmentSize + ptr.fileOffset() - prevPtr.fileOffset();

            double segmentsCnt = estimatedSize / walSegmentSize;

            points.add(new BenchmarkProbePoint(TimeUnit.MILLISECONDS.toSeconds(time), new double[] {segmentsCnt, estimatedSize}));

            prevPtr = ptr;
        }
        catch (IgniteCheckedException ignore) {
            // No-op.
        }
    }
}
