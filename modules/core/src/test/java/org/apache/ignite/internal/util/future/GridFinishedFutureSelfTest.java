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

import org.apache.ignite.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.marshaller.*;
import org.apache.ignite.marshaller.optimized.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.testframework.*;
import org.apache.ignite.testframework.junits.common.*;
import org.jetbrains.annotations.*;

import java.util.concurrent.*;

import static java.util.concurrent.TimeUnit.*;

/**
 * Tests finished future use cases.
 */
public class GridFinishedFutureSelfTest extends GridCommonAbstractTest {
    /** Create test and start grid. */
    public GridFinishedFutureSelfTest() {
        super(true);
    }

    /**
     * Test finished future serialization.
     *
     * @throws Exception In case of any exception.
     */
    public void testExternalizable() throws Exception {
        Object t = "result";
        Throwable ex = new IgniteException("exception");

        testExternalizable(t, null, true);
        testExternalizable(t, null, false);
        testExternalizable(null, ex, true);
        testExternalizable(null, ex, false);
    }

    /**
     * Test finished future serialization.
     *
     * @param t Future result.
     * @param ex Future exception.
     * @param syncNotify Synchronous notifications flag.
     * @throws Exception In case of any exception.
     */
    private void testExternalizable(@Nullable Object t, @Nullable Throwable ex, boolean syncNotify) throws Exception {
        GridKernalContext ctx = ((IgniteKernal)grid()).context();

        IgniteMarshaller m = new IgniteOptimizedMarshaller();
        ClassLoader clsLdr = getClass().getClassLoader();

        IgniteInternalFuture<Object> orig = t == null ? new GridFinishedFuture<>(ctx, ex) :
            new GridFinishedFuture<>(ctx, t);

        orig.syncNotify(syncNotify);

        GridFinishedFuture<Object> fut = m.unmarshal(m.marshal(orig), clsLdr);

        assertEquals(t, GridTestUtils.<Object>getFieldValue(fut, "t"));

        if (ex == null)
            assertNull(GridTestUtils.<Throwable>getFieldValue(fut, "err"));
        else {
            assertEquals(ex.getClass(), GridTestUtils.<Throwable>getFieldValue(fut, "err").getClass());
            assertEquals(ex.getMessage(), GridTestUtils.<Throwable>getFieldValue(fut, "err").getMessage());
        }

        assertEquals(syncNotify, GridTestUtils.<Boolean>getFieldValue(fut, "syncNotify").booleanValue());
        assertEquals(ctx.gridName(), GridTestUtils.<GridKernalContext>getFieldValue(fut, "ctx").gridName());

        final CountDownLatch done = new CountDownLatch(1);

        fut.listenAsync(new CI1<IgniteInternalFuture<Object>>() {
            @Override public void apply(IgniteInternalFuture<Object> t) {
                done.countDown();
            }
        });

        if (syncNotify)
            assertEquals("Expect notification is already complete.", 0, done.getCount());
        else
            assertTrue("Wait until notification completes asynchronously.", done.await(100, MILLISECONDS));
    }
}
