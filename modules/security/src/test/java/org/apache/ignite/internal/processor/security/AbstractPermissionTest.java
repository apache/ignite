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

package org.apache.ignite.internal.processor.security;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import org.apache.ignite.Ignite;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.plugin.security.SecurityException;

import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsNull.nullValue;
import static org.junit.Assert.assertThat;

/**
 *
 */
public abstract class AbstractPermissionTest extends AbstractSecurityTest {
    /** Values. */
    protected AtomicInteger values = new AtomicInteger(0);

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGrid("server", allowAllPermissionSet()).cluster().active(true);
    }

    /**
     * Getting login prefix.
     *
     * @param isClient True if is client mode.
     * @return Prefix.
     */
    protected String loginPrefix(boolean isClient) {
        return isClient ? "client" : "server";
    }

    /**
     * @return Cache entry for test.
     */
    protected T2<String, Integer> entry() {
        int val = values.incrementAndGet();

        return new T2<>("key_" + val, -1 * val);
    }

    /**
     * @param c Consumer.
     */
    protected void assertAllowed(Ignite validator, String cacheName, Consumer<T2<String, Integer>> c) {
        T2<String, Integer> entry = entry();

        c.accept(entry);

        assertThat(validator.cache(cacheName).get(entry.getKey()), is(entry.getValue()));
    }

    /**
     * @param c Consumer.
     */
    protected void assertForbidden(Ignite validator, String cacheName, Consumer<T2<String, Integer>> c) {
        T2<String, Integer> entry = entry();

        try {
            c.accept(entry);

            fail("Should not happen.");
        }
        catch (Throwable e) {
            assertThat(X.cause(e, SecurityException.class), notNullValue());
        }

        assertThat(validator.cache(cacheName).get(entry.getKey()), nullValue());
    }
}
