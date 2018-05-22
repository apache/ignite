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

package org.apache.ignite.internal.util.future;

import javax.cache.CacheException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.cache.IgniteCacheFutureImpl;

/**
 * Tests IgniteCacheFutureImpl.
 */
public class IgniteCacheFutureImplTest extends IgniteFutureImplTest {
    /** {@inheritDoc} */
    @Override protected <V> IgniteFutureImpl<V> createFuture(IgniteInternalFuture<V> fut) {
        return new IgniteCacheFutureImpl<>(fut);
    }

    /** {@inheritDoc} */
    @Override protected Class<? extends Exception> expectedException() {
        return CacheException.class;
    }

    /** {@inheritDoc} */
    @Override protected void assertExpectedException(Exception e, Exception exp) {
        if (exp instanceof IgniteException)
            assertEquals(exp, e.getCause().getCause());
        else
            assertEquals(exp, e.getCause());
    }
}
