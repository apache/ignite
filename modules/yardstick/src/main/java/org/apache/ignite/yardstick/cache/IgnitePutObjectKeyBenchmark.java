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

package org.apache.ignite.yardstick.cache;

import java.io.Serializable;
import java.util.Map;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.yardstick.cache.model.SampleValue;
import org.yardstickframework.BenchmarkConfiguration;

/**
 *
 */
public class IgnitePutObjectKeyBenchmark extends IgniteCacheAbstractBenchmark<Object, Object> {
    /** {@inheritDoc} */
    @Override public void setUp(BenchmarkConfiguration cfg) throws Exception {
        super.setUp(cfg);
    }

    /** {@inheritDoc} */
    @Override public boolean test(Map<Object, Object> ctx) throws Exception {
        int key = nextRandom(args.range());

        IgniteCache<Object, Object> cache = cacheForOperation();

        cache.put(grpCaches != null ? new Key1(key) : new Key2(key, key), new SampleValue(key));

        return true;
    }

    /** {@inheritDoc} */
    @Override protected IgniteCache<Object, Object> cache() {
        return ignite().cache("atomic");
    }

    /**
     *
     */
    static class Key1 implements Serializable {
        /** */
        private final int id;

        /**
         * @param id ID.
         */
        Key1(int id) {
            this.id = id;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            Key1 key1 = (Key1)o;

            return id == key1.id;
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return id;
        }
    }

    /**
     *
     */
    static class Key2 implements Serializable {
        /** */
        private final int id1;

        /** */
        private final int id2;

        /**
         * @param id1 ID1.
         * @param id2 ID2.
         */
        Key2(int id1, int id2) {
            this.id1 = id1;
            this.id2 = id2;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            Key2 key2 = (Key2) o;

            return id1 == key2.id1 && id2 == key2.id2;
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            int res = id1;

            res = 31 * res + id2;

            return res;
        }
    }
}
