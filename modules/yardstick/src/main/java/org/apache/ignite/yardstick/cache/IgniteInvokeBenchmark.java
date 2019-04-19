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

package org.apache.ignite.yardstick.cache;

import java.util.Map;
import javax.cache.processor.MutableEntry;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheEntryProcessor;
import org.apache.ignite.yardstick.cache.model.SampleValue;

/**
 * Ignite benchmark that performs invoke operations.
 */
public class IgniteInvokeBenchmark extends IgniteCacheAbstractBenchmark<Integer, Object> {
    /** {@inheritDoc} */
    @Override public boolean test(Map<Object, Object> ctx) throws Exception {
        int key = nextRandom(args.range());

        IgniteCache<Integer, Object> cache = cacheForOperation();

        cache.invoke(key, new SetValueEntryProcessor(new SampleValue(key)));

        return true;
    }

    /** {@inheritDoc} */
    @Override protected IgniteCache<Integer, Object> cache() {
        return ignite().cache("atomic");
    }

    /**
     *
     */
    public static class SetValueEntryProcessor implements CacheEntryProcessor<Integer, Object, Object> {
        /** */
        private Object val;

        /**
         * @param val Value.
         */
        public SetValueEntryProcessor(Object val) {
            this.val = val;
        }

        /** {@inheritDoc} */
        @Override public Object process(MutableEntry<Integer, Object> entry, Object... args) {
            entry.setValue(val);

            return null;
        }
    }
}
