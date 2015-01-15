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

package org.gridgain.testframework.junits.cache;

import org.apache.ignite.cache.store.*;
import org.apache.ignite.transactions.*;
import org.jetbrains.annotations.*;

import java.util.*;

/**
 *
 */
public class TestThreadLocalCacheSession implements CacheStoreSession {
    /** */
    private final ThreadLocal<TestCacheSession> sesHolder = new ThreadLocal<>();

    /**
     * @param tx Transaction.
     */
    public void newSession(@Nullable IgniteTx tx) {
        TestCacheSession ses = new TestCacheSession();

        ses.newSession(tx);

        sesHolder.set(ses);
    }

    /** {@inheritDoc} */
    @Nullable @Override public IgniteTx transaction() {
        TestCacheSession ses = sesHolder.get();

        return ses != null ? ses.transaction() : null;
    }

    /** {@inheritDoc} */
    @Override public Map<Object, Object> properties() {
        TestCacheSession ses = sesHolder.get();

        return ses != null ? ses.properties() : null;
    }
}
