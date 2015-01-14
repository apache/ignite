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

package org.apache.ignite.internal.processors.cache.integration;

import org.apache.ignite.internal.processors.cache.*;

import javax.cache.*;
import javax.cache.configuration.*;
import javax.cache.integration.*;
import java.util.*;

/**
 *
 */
public abstract class IgniteCacheLoaderWriterAbstractTest extends IgniteCacheAbstractTest {
    /** {@inheritDoc} */
    @Override protected Factory<? extends CacheLoader> loaderFactory() {
        return super.loaderFactory();
    }

    /** {@inheritDoc} */
    @Override protected Factory<? extends CacheWriter> writerFactory() {
        return super.writerFactory();
    }

    /**
     * @throws Exception If failed.
     */
    public void testLoad() throws Exception {
    }

    /**
     *
     */
    static class TestLoader implements CacheLoader<Integer, Integer> {
        /** {@inheritDoc} */
        @Override public Integer load(Integer key) {
            return null;
        }

        /** {@inheritDoc} */
        @Override public Map<Integer, Integer> loadAll(Iterable<? extends Integer> keys) {
            return null;
        }
    }

    /**
     *
     */
    static class TestWriter implements CacheWriter<Integer, Integer> {
        /** {@inheritDoc} */
        @Override public void write(Cache.Entry<? extends Integer, ? extends Integer> entry) {
        }

        /** {@inheritDoc} */
        @Override public void writeAll(Collection<Cache.Entry<? extends Integer, ? extends Integer>> entries) {
        }

        /** {@inheritDoc} */
        @Override public void delete(Object key) {
        }

        /** {@inheritDoc} */
        @Override public void deleteAll(Collection<?> keys) {
        }
    }
}
