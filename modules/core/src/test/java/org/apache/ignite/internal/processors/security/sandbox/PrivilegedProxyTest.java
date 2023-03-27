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

package org.apache.ignite.internal.processors.security.sandbox;

import java.lang.reflect.Proxy;
import java.security.AccessControlException;
import java.util.Collections;
import org.apache.ignite.Ignition;
import org.apache.ignite.internal.util.lang.GridIterableAdapter;
import org.apache.ignite.internal.util.lang.GridIterator;
import org.junit.Test;

/**
 * Test that user defined classes can't be wrapped into priveleged proxy.
 */
public class PrivilegedProxyTest extends AbstractSandboxTest {
    /** */
    @Test
    public void testPrivelegedUserObject() throws Exception {
        grid(CLNT_FORBIDDEN_WRITE_PROP).getOrCreateCache(DEFAULT_CACHE_NAME).put(0, new TestIterator<>());

        runForbiddenOperation(() -> grid(CLNT_FORBIDDEN_WRITE_PROP).compute().run(() -> {
            GridIterator<?> it = (GridIterator<?>)Ignition.localIgnite().cache(DEFAULT_CACHE_NAME).get(0);

            // User object should not be proxied with privileged proxy.
            assertFalse(Proxy.isProxyClass(it.getClass()));

            it.iterator();
        }), AccessControlException.class);
    }

    /** */
    public static class TestIterator<T> extends GridIterableAdapter<T> {
        /** */
        public TestIterator() {
            super(Collections.emptyIterator());
        }

        /** {@inheritDoc} */
        @Override public GridIterator<T> iterator() {
            controlAction();

            return super.iterator();
        }
    }
}
