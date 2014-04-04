/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.jvmtest;

import junit.framework.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.testframework.*;
import org.jetbrains.annotations.*;

import java.net.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

/**
 * Java server socket test.
 * <p>
 * http://gridgain.jira.com/browse/GG-2068
 * <p>
 * When binding server socket to the same port in multiple threads, either
 * BindException or SocketException may be thrown. Purpose of this test is
 * to find some explanation to that.
 */
public class ServerSocketMultiThreadedTest extends TestCase {
    /** */
    private static final int THREADS_CNT = 10;

    /** */
    private static final int ITER_CNT = 10000;

    /**
     * @throws Exception If failed.
     */
    public void testConcurrentBind() throws Exception {
        final AtomicInteger bindExCnt = new AtomicInteger();
        final AtomicInteger sockExCnt = new AtomicInteger();
        final AtomicInteger okCnt = new AtomicInteger();

        final CyclicBarrier finishBarrier = new CyclicBarrier(
            THREADS_CNT,
            new Runnable() {
                private int i;

                @Override public void run() {
                    if (++i % 250 == 0)
                        X.println("Finished iteration [threadName=" + Thread.currentThread().getName() +
                            ", iter=" + i + ']');
                }
            });

        final InetAddress addr = InetAddress.getByName("127.0.0.1");

        GridTestUtils.runMultiThreaded(
            new Callable<Object>() {
                @Nullable @Override public Object call() throws Exception {
                    ServerSocket srvSock = null;

                    for (int i = 0; i < ITER_CNT; i++) {
                        try {
                            srvSock = new ServerSocket(60000, 0, addr);

                            okCnt.incrementAndGet();
                        }
                        catch (BindException ignore) {
                            bindExCnt.incrementAndGet();
                        }
                        catch (SocketException ignore) {
                            sockExCnt.incrementAndGet();
                        }
                        finally {
                            finishBarrier.await();

                            U.closeQuiet(srvSock);
                        }
                    }

                    return null;
                }
            },
            THREADS_CNT,
            "binder"
        );

        X.println("Test stats [bindExCnt=" + bindExCnt.get() + ", sockExCnt=" + sockExCnt.get() +
            ", okCnt=" + okCnt + ']');
    }
}
