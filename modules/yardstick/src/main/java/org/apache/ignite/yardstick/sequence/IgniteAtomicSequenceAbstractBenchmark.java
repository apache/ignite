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

package org.apache.ignite.yardstick.sequence;

import org.apache.ignite.IgniteAtomicSequence;
import org.apache.ignite.configuration.AtomicConfiguration;
import org.apache.ignite.yardstick.IgniteAbstractBenchmark;
import org.yardstickframework.BenchmarkConfiguration;

/**
 * Abstract class for {@link IgniteAtomicSequence} benchmarks.
 */
public abstract class IgniteAtomicSequenceAbstractBenchmark extends IgniteAbstractBenchmark {
    /** Bound for random operation, by default 1/10 of batchSize. */
    protected int randomBound;

    /** Sequence. */
    protected IgniteAtomicSequence seq;

    /** {@inheritDoc} */
    @Override public void setUp(BenchmarkConfiguration cfg) throws Exception {
        super.setUp(cfg);

        int batchSize = args.batch();
        int backups = args.backups();

        AtomicConfiguration acfg = new AtomicConfiguration();

        acfg.setAtomicSequenceReserveSize(batchSize);
        acfg.setBackups(backups);

        seq = ignite().atomicSequence("benchSequence", acfg, 0, true);

        randomBound = batchSize < 10 ? 1 : batchSize / 10;
    }
}
