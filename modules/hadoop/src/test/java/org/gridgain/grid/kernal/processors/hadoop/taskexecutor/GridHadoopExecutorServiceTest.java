package org.gridgain.grid.kernal.processors.hadoop.taskexecutor;

import org.gridgain.grid.IgniteFuture;
import org.gridgain.grid.util.typedef.X;
import org.gridgain.testframework.junits.common.GridCommonAbstractTest;
import org.jdk8.backport.LongAdder;

import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 *
 */
public class GridHadoopExecutorServiceTest extends GridCommonAbstractTest {
    /**
     * @throws Exception If failed.
     */
    public void testExecutesAll() throws Exception {
        final GridHadoopExecutorService exec = new GridHadoopExecutorService(log, "_GRID_NAME_", 10, 5);

        for (int i = 0; i < 5; i++) {
            final int loops = 5000;
            int threads = 17;

            final LongAdder sum = new LongAdder();

            multithreaded(new Callable<Object>() {
                @Override public Object call() throws Exception {
                    for (int i = 0; i < loops; i++) {
                        exec.submit(new Callable<Void>() {
                            @Override
                            public Void call() throws Exception {
                                sum.increment();

                                return null;
                            }
                        });
                    }

                    return null;
                }
            }, threads);

            while (exec.active() != 0) {
                X.println("__ active: " + exec.active());

                Thread.sleep(200);
            }

            assertEquals(threads * loops, sum.sum());

            X.println("_ ok");
        }

        assertTrue(exec.shutdown(0));
    }

    /**
     * @throws Exception If failed.
     */
    public void testShutdown() throws Exception {
        for (int i = 0; i < 5; i++) {
            final GridHadoopExecutorService exec = new GridHadoopExecutorService(log, "_GRID_NAME_", 10, 5);

            final LongAdder sum = new LongAdder();

            final AtomicBoolean finish = new AtomicBoolean();

            IgniteFuture<?> fut = multithreadedAsync(new Callable<Object>() {
                @Override public Object call() throws Exception {
                    while (!finish.get()) {
                        exec.submit(new Callable<Void>() {
                            @Override public Void call() throws Exception {
                                sum.increment();

                                return null;
                            }
                        });
                    }

                    return null;
                }
            }, 19);

            Thread.sleep(200);

            assertTrue(exec.shutdown(50));

            long res = sum.sum();

            assertTrue(res > 0);

            finish.set(true);

            fut.get();

            assertEquals(res, sum.sum()); // Nothing was executed after shutdown.

            X.println("_ ok");
        }
    }
}
