/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.port;

import org.apache.ignite.spi.communication.tcp.*;
import org.gridgain.testframework.junits.*;
import org.gridgain.testframework.junits.common.*;

import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import static org.apache.ignite.spi.IgnitePortProtocol.*;

/**
 *
 */
public class GridPortProcessorSelfTest extends GridCommonAbstractTest {
    /** */
    private GridTestKernalContext ctx;

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        ctx = newContext();

        ctx.add(new GridPortProcessor(ctx));

        ctx.start();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        ctx.stop(true);

        ctx = null;
    }

    /**
     * @throws Exception If any exception occurs.
     */
    public void testA() throws Exception {
        Class cls1 = TcpCommunicationSpi.class;

        ctx.ports().registerPort(40000, TCP, cls1);
        ctx.ports().registerPort(40000, TCP, cls1);

        Class cls2 = GridPortProcessorSelfTest.class;

        ctx.ports().registerPort(30100, TCP, cls2);
        ctx.ports().registerPort(30200, UDP, cls2);

        assertEquals(3, ctx.ports().records().size());

        ctx.ports().deregisterPort(30100, UDP, cls2);

        assertEquals(3, ctx.ports().records().size());

        ctx.ports().deregisterPorts(cls2);

        assertEquals(1, ctx.ports().records().size());

        ctx.ports().deregisterPort(40000, TCP, cls1);

        assert ctx.ports().records().isEmpty();
    }

    /**
     * @throws Exception If any exception occurs.
     */
    public void testB() throws Exception {
        final AtomicInteger ai = new AtomicInteger();

        ctx.ports().addPortListener(new GridPortListener() {
            @Override public void onPortChange() {
                ai.incrementAndGet();
            }
        });

        final int cnt = 5;

        final CountDownLatch beginLatch = new CountDownLatch(1);

        final CountDownLatch finishLatch = new CountDownLatch(cnt);

        for (int i = 1; i <= cnt; i++) {
            final int k = i * cnt;

            new Thread() {
                @Override public void run() {
                    try {
                        beginLatch.await();
                    }
                    catch (InterruptedException ignore) {
                        assertTrue(false);
                    }

                    for (int j = 1; j <= cnt; j++)
                        ctx.ports().registerPort(j + k, TCP, TcpCommunicationSpi.class);

                    finishLatch.countDown();
                }
            }.start();
        }

        beginLatch.countDown();

        finishLatch.await();

        int i = cnt * cnt;

        assertEquals(i, ctx.ports().records().size());

        assertEquals(i, ai.get());
    }
}
