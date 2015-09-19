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

package org.apache.ignite.internal.processors.cache.datastructures;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteAtomicSequence;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.util.typedef.CAX;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.resources.LoggerResource;

/**
 * Sequence multi node tests.
 */
public abstract class GridCacheSequenceMultiNodeAbstractSelfTest extends IgniteAtomicsAbstractTest
    implements Externalizable {
    /** */
    protected static final int GRID_CNT = 4;

    /**  */
    protected static final int BATCH_SIZE = 33;

    /** */
    protected static final int RETRIES = 1111;

    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return GRID_CNT;
    }

    /**
     * JUnit.
     *
     * @throws Exception If failed.
     */
    public void testIncrementAndGet() throws Exception {
        Collection<Long> res = new HashSet<>();

        String seqName = UUID.randomUUID().toString();

        for (int i = 0; i < GRID_CNT; i++) {
            Set<Long> retVal = compute(grid(i).cluster().forLocal()).
                call(new IncrementAndGetJob(seqName, RETRIES));

            for (Long l : retVal)
                assert !res.contains(l) : "Value already was used " + l;

            res.addAll(retVal);
        }

        assert res.size() == GRID_CNT * RETRIES;

        int gapSize = 0;

        for (long i = 1; i <= GRID_CNT * RETRIES; i++) {
            if (!res.contains(i))
                gapSize++;
            else
                gapSize = 0;

            assert gapSize <= BATCH_SIZE :
                "Gap above id  " + i + " is " + gapSize + "more than batch size: " + BATCH_SIZE;
        }
    }

    /**
     * JUnit.
     *
     * @throws Exception If failed.
     */
    public void testGetAndIncrement() throws Exception {
        Collection<Long> res = new HashSet<>();

        String seqName = UUID.randomUUID().toString();

        for (int i = 0; i < GRID_CNT; i++) {
            Set<Long> retVal = compute(grid(i).cluster().forLocal()).
                call(new GetAndIncrementJob(seqName, RETRIES));

            for (Long l : retVal)
                assert !res.contains(l) : "Value already was used " + l;

            res.addAll(retVal);
        }

        assert res.size() == GRID_CNT * RETRIES;

        int gapSize = 0;

        for (long i = 0; i < GRID_CNT * RETRIES; i++) {
            if (!res.contains(i))
                gapSize++;
            else
                gapSize = 0;

            assert gapSize <= BATCH_SIZE + 1 :
                "Gap above id  " + i + " is " + gapSize + " more than batch size: " + (BATCH_SIZE + 1);
        }
    }

    /**
     * JUnit.
     *
     * @throws Exception If failed.
     */
    public void testMarshalling() throws Exception {
        String seqName = UUID.randomUUID().toString();

        final IgniteAtomicSequence seq = grid(0).atomicSequence(seqName, 0, true);

        grid(1).compute().run(new CAX() {
            @Override public void applyx() {
                assertNotNull(seq);

                for (int i = 0; i < RETRIES; i++)
                    seq.incrementAndGet();
            }
        });
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        // No-op.
    }

    /**
     * Test job with method cache name annotation.
     */
    private static class IncrementAndGetJob implements IgniteCallable<Set<Long>> {
        /** */
        @IgniteInstanceResource
        private Ignite ignite;

        /** */
        @LoggerResource
        private IgniteLogger log;

        /** Sequence name. */
        private final String seqName;

        /** */
        private final int retries;

        /**
         * @param seqName Sequence name.
         * @param retries  Number of operations.
         */
        IncrementAndGetJob(String seqName, int retries) {
            this.seqName = seqName;
            this.retries = retries;
        }

        /** {@inheritDoc} */
        @Override public Set<Long> call() throws IgniteCheckedException {
            assert ignite != null;

            if (log.isInfoEnabled())
                log.info("Running IncrementAndGetJob on node: " + ignite.cluster().localNode().id());

            IgniteAtomicSequence seq = ignite.atomicSequence(seqName, 0, true);

            assert seq != null;

            // Result set.
            Set<Long> resSet = new HashSet<>();

            // Get sequence value and try to put it result set.
            for (int i = 0; i < retries; i++) {
                long val = seq.incrementAndGet();

                assert !resSet.contains(val) : "Element already in set : " + val;

                resSet.add(val);
            }

            return resSet;
        }
    }

    /**
     * Test job with method cache name annotation.
     */
    private static class GetAndIncrementJob implements IgniteCallable<Set<Long>> {
        /** */
        @IgniteInstanceResource
        private Ignite ignite;

        /** */
        @LoggerResource
        private IgniteLogger log;

        /** Sequence name. */
        private final String seqName;

        /** */
        private final int retries;

        /**
         * @param seqName Sequence name.
         * @param retries  Number of operations.
         */
        GetAndIncrementJob(String seqName, int retries) {
            this.seqName = seqName;
            this.retries = retries;
        }

        /** {@inheritDoc} */
        @Override public Set<Long> call() throws IgniteCheckedException {
            assert ignite != null;

            if (log.isInfoEnabled())
                log.info("Running GetAndIncrementJob on node: " + ignite.cluster().localNode().id());

            IgniteAtomicSequence seq = ignite.atomicSequence(seqName, 0, true);

            assert seq != null;

            // Result set.
            Set<Long> resSet = new HashSet<>();

            // Get sequence value and try to put it result set.
            for (int i = 0; i < retries; i++) {
                long val = seq.getAndIncrement();

                assert !resSet.contains(val) : "Element already in set : " + val;

                resSet.add(val);
            }

            return resSet;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(GetAndIncrementJob.class, this);
        }
    }
}