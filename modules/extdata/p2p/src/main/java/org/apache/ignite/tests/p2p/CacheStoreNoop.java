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

package org.apache.ignite.tests.p2p;

import org.apache.ignite.cache.store.*;

import javax.cache.*;
import javax.cache.integration.*;
import java.util.concurrent.atomic.*;

/**
 *
 */
public class CacheStoreNoop extends CacheStoreAdapter {
    /** */
    private static final AtomicInteger loadCnt = new AtomicInteger(0);

    /** */
    private static final AtomicInteger writeCnt = new AtomicInteger(0);

    /** */
    private static final AtomicInteger deleteCnt = new AtomicInteger(0);

    /** */
    private static boolean isInjected;

    /** {@inheritDoc} */
    @Override public Object load(Object key) throws CacheLoaderException {
        loadCnt.incrementAndGet();

        isInjected = ignite() != null;

        return 42L;
    }

    /** {@inheritDoc} */
    @Override public void write(Cache.Entry entry) throws CacheWriterException {
        writeCnt.incrementAndGet();
    }

    /** {@inheritDoc} */
    @Override public void delete(Object key) throws CacheWriterException {
        deleteCnt.incrementAndGet();
    }

    /**
     *
     */
    public static int getLoadCnt() {
        return loadCnt.get();
    }

    /**
     *
     */
    public static int getWriteCnt() {
        return writeCnt.get();
    }

    /**
     *
     */
    public static int getDeleteCnt() {
        return deleteCnt.get();
    }

    /**
     *
     */
    public boolean isInjected() {
        return isInjected;
    }
}
