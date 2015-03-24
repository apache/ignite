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

import org.apache.ignite.*;
import org.apache.ignite.cache.*;
import org.apache.ignite.internal.processors.task.*;
import org.apache.ignite.internal.util.lang.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.internal.visor.*;

import javax.cache.expiry.*;
import java.util.*;
import java.util.concurrent.*;

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
        @Override protected Map<String, Integer> run(GridTuple3<Set<String>, Long, Object[]> arg) {
            Set<String> cacheNames = arg.get1();
            Long ttl = arg.get2();
            Object[] ldrArgs = arg.get3();

            assert cacheNames != null && !cacheNames.isEmpty();
            assert ttl != null;

            Map<String, Integer> res = U.newHashMap(cacheNames.size());

            ExpiryPolicy plc = null;

            for (String cacheName : cacheNames) {
                IgniteCache cache = ignite.cache(cacheName);

                if (ttl > 0) {
                    if (plc == null)
                        plc = new CreatedExpiryPolicy(new Duration(TimeUnit.MILLISECONDS, ttl));

                    cache = cache.withExpiryPolicy(plc);
                }

                cache.loadCache(null, ldrArgs);

                res.put(cacheName, cache.size(CachePeekMode.PRIMARY));
            }

            return res;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(VisorCachesLoadJob.class, this);
        }
    }
}
