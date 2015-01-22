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

package org.gridgain.grid.kernal.visor.cache;

import org.apache.ignite.*;
import org.apache.ignite.cache.*;
import org.gridgain.grid.kernal.processors.task.*;
import org.gridgain.grid.kernal.visor.*;
import org.gridgain.grid.util.lang.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;

import java.util.*;

/**
 * Task to loads caches.
 */
@GridInternal
public class VisorCacheLoadTask extends
    VisorOneNodeTask<GridTuple3<Set<String>, Long, Object[]>, Map<String, Integer>> {
    /** */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Override protected VisorCachesLoadJob job(GridTuple3<Set<String>, Long, Object[]> arg) {
        return new VisorCachesLoadJob(arg, debug);
    }

    /** Job that load caches. */
    private static class VisorCachesLoadJob extends
        VisorJob<GridTuple3<Set<String>, Long, Object[]>, Map<String, Integer>> {
        /** */
        private static final long serialVersionUID = 0L;

        /**
         * @param arg Cache names, ttl and loader arguments.
         * @param debug Debug flag.
         */
        private VisorCachesLoadJob(GridTuple3<Set<String>, Long, Object[]> arg, boolean debug) {
            super(arg, debug);
        }

        /** {@inheritDoc} */
        @Override protected Map<String, Integer> run(GridTuple3<Set<String>, Long, Object[]> arg) throws IgniteCheckedException {
            Set<String> cacheNames = arg.get1();
            Long ttl = arg.get2();
            Object[] loaderArgs = arg.get3();

            Map<String, Integer> res = new HashMap<>();

            for (GridCache c: g.cachesx()) {
                String cacheName = c.name();

                if (cacheNames.contains(cacheName)) {
                    c.loadCache(new P2<Object, Object>() {
                        @Override public boolean apply(Object o, Object o2) {
                            return true;
                        }
                    }, ttl, loaderArgs);

                    res.put(cacheName, c.size()); // Put new key size for successfully loaded cache.
                }
            }

            return res;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(VisorCachesLoadJob.class, this);
        }
    }
}
