/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.util.future;

import org.apache.ignite.lang.*;
import org.apache.ignite.marshaller.*;
import org.apache.ignite.marshaller.optimized.*;
import org.gridgain.grid.*;
import org.gridgain.grid.kernal.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.testframework.*;
import org.gridgain.testframework.junits.common.*;
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
        Throwable ex = new GridRuntimeException("exception");

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
        GridKernalContext ctx = ((GridKernal)grid()).context();

        GridMarshaller m = new IgniteOptimizedMarshaller();
        ClassLoader clsLdr = getClass().getClassLoader();

        IgniteFuture<Object> orig = t == null ? new GridFinishedFuture<>(ctx, ex) :
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

        fut.listenAsync(new CI1<IgniteFuture<Object>>() {
            @Override public void apply(IgniteFuture<Object> t) {
                done.countDown();
            }
        });

        if (syncNotify)
            assertEquals("Expect notification is already complete.", 0, done.getCount());
        else
            assertTrue("Wait until notification completes asynchronously.", done.await(100, MILLISECONDS));
    }
}
