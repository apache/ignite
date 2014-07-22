/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.spi.indexing.h2;

import org.gridgain.grid.*;
import org.gridgain.grid.spi.*;
import org.gridgain.grid.util.lang.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.testframework.junits.common.*;

import java.io.*;
import java.lang.reflect.*;
import java.sql.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

/**
 * Test for {@link GridH2ResultSetIterator}.
 */
public class GridH2ResultSetIteratorSelfTest extends GridCommonAbstractTest {
    /**
     * @throws Exception If failed.
     */
    public void testConcurrentClose() throws Exception {
        final Random rnd = new Random();

        for (int i = 0; i < 30; i++) {
            info("Iteration " + i);

            Object proxy = Proxy.newProxyInstance(getClass().getClassLoader(),
                new Class[] {ResultSet.class, Statement.class}, new InvocationHandler() {
                    private volatile boolean closed;

                    @Override public Object invoke(Object o, Method method, Object[] objects) throws Throwable {
                        if ("close".equals(method.getName()))
                            closed = true;
                        else if ("isClosed".equals(method.getName()))
                            return closed;
                        else if ("next".equals(method.getName()))
                            return true;

                        return null;
                    }
                });

            final GridSpiCloseableIterator<Integer> it = new TestIterator(
                GridH2IndexingSpi.fetchResult(F.t((Statement)proxy, (ResultSet)proxy)));

            GridFuture<?> fut = multithreadedAsync(new Callable<Object>() {
                @Override public Object call() throws Exception {
                    Thread.sleep(500 + rnd.nextInt(500));

                    it.close();

                    return null;
                }
            }, 1);

            while (it.hasNext())
                it.next();

            fut.get();
        }
    }

    /**
     * Iterator implementation.
     */
    private static class TestIterator extends GridH2ResultSetIterator<Integer> {
        /** */
        private final AtomicInteger val =  new AtomicInteger();

        /**
         * @param data Data array.
         */
        protected TestIterator(Object[][] data) {
            super(data);
        }

        /** {@inheritDoc} */
        @Override protected Integer createRow(Object[] row) {
            return val.getAndIncrement();
        }
    }
}
