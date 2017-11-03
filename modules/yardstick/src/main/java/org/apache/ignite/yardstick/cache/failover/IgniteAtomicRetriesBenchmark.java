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

package org.apache.ignite.yardstick.cache.failover;

import java.util.Map;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.MutableEntry;
import org.apache.ignite.cache.CacheEntryProcessor;

/**
 * Atomic retries failover benchmark.
 * <p>
 * Client generates continuous load to the cluster (random get, put, invoke, remove
 * operations).
 */
public class IgniteAtomicRetriesBenchmark extends IgniteFailoverAbstractBenchmark<Integer, String> {
    /** {@inheritDoc} */
    @Override public boolean test(Map<Object, Object> ctx) throws Exception {
        final int key = nextRandom(args.range());

        int opNum = nextRandom(4);

        final int timeout = args.cacheOperationTimeoutMillis();

        switch (opNum) {
            case 0:
                cache.getAsync(key).get(timeout);

                break;

            case 1:
                cache.putAsync(key, String.valueOf(key)).get(timeout);

                break;

            case 2:
                cache.invokeAsync(key, new TestCacheEntryProcessor()).get(timeout);

                break;

            case 3:
                cache.removeAsync(key).get(timeout);

                break;

            default:
                throw new IllegalStateException("Got invalid operation number: " + opNum);
        }

        return true;
    }

    /** {@inheritDoc} */
    @Override protected String cacheName() {
        return "atomic-reties";
    }

    /**
     */
    private static class TestCacheEntryProcessor implements CacheEntryProcessor<Integer, String, String> {
        /** Serial version uid. */
        private static final long serialVersionUID = 0;

        /** {@inheritDoc} */
        @Override public String process(MutableEntry<Integer, String> entry,
            Object... arguments) throws EntryProcessorException {
            return "key";
        }
    }
}
