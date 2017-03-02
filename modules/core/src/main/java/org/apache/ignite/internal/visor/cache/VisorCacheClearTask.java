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

package org.apache.ignite.internal.visor.cache;

import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.compute.ComputeJobContext;
import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.visor.VisorJob;
import org.apache.ignite.internal.visor.VisorOneNodeTask;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.resources.JobContextResource;

/**
 * Task that clears specified caches on specified node.
 */
@GridInternal
public class VisorCacheClearTask extends VisorOneNodeTask<String, IgniteBiTuple<Integer, Integer>> {
    /** */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Override protected VisorCacheClearJob job(String arg) {
        return new VisorCacheClearJob(arg, debug);
    }

    /**
     * Job that clear specified caches.
     */
    private static class VisorCacheClearJob extends VisorJob<String, IgniteBiTuple<Integer, Integer>> {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        private final String cacheName;

        /** */
        private final IgniteInClosure<IgniteFuture<Integer>> lsnr;

        /** */
        private IgniteFuture<Integer>[] futs;

        /** */
        @JobContextResource
        private ComputeJobContext jobCtx;

        /**
         * Create job.
         *
         * @param cacheName Cache name to clear.
         * @param debug Debug flag.
         */
        private VisorCacheClearJob(String cacheName, boolean debug) {
            super(cacheName, debug);

            this.cacheName = cacheName;

            lsnr = new IgniteInClosure<IgniteFuture<Integer>>() {
                /** */
                private static final long serialVersionUID = 0L;

                @Override public void apply(IgniteFuture<Integer> f) {
                    assert futs[0].isDone();
                    assert futs[1] == null || futs[1].isDone();
                    assert futs[2] == null || futs[2].isDone();

                    jobCtx.callcc();
                }
            };
        }

        /**
         * @param fut Future for asynchronous cache operation.
         * @param idx Index.
         * @return {@code true} If subJob was not completed and this job should be suspended.
         */
        private boolean callAsync(IgniteFuture<Integer> fut, int idx) {
            futs[idx] = fut;

            if (fut.isDone())
                return false;

            jobCtx.holdcc();

            fut.listen(lsnr);

            return true;
        }

        /** {@inheritDoc} */
        @Override protected IgniteBiTuple<Integer, Integer> run(final String cacheName) {
            if (futs == null)
                futs = new IgniteFuture[3];

            if (futs[0] == null || futs[1] == null || futs[2] == null) {
                IgniteCache cache = ignite.cache(cacheName).withAsync();

                if (futs[0] == null) {
                    cache.size(CachePeekMode.PRIMARY);

                    if (callAsync(cache.<Integer>future(), 0))
                        return null;
                }

                if (futs[1] == null) {
                    cache.clear();

                    if (callAsync(cache.<Integer>future(), 1))
                        return null;
                }
                
                if (futs[2] == null) {
                    cache.size(CachePeekMode.PRIMARY);

                    if (callAsync(cache.<Integer>future(), 2))
                        return null;
                }
            }

            assert futs[0].isDone() && futs[1].isDone() && futs[2].isDone();

            return new IgniteBiTuple<>(futs[0].get(), futs[2].get());
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(VisorCacheClearJob.class, this);
        }
    }
}