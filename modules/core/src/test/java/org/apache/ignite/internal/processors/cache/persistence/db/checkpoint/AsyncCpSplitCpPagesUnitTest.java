package org.apache.ignite.internal.processors.cache.persistence.db.checkpoint;

import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinTask;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.pagemem.FullPageId;
import org.apache.ignite.internal.processors.cache.persistence.checkpoint.AsyncCheckpointer;
import org.apache.ignite.internal.util.GridMultiCollectionWrapper;
import org.apache.ignite.internal.util.future.CountDownFuture;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.lang.IgniteBiClosure;
import org.apache.ignite.logger.NullLogger;
import org.junit.AfterClass;
import org.junit.Test;

import static org.apache.ignite.internal.processors.cache.persistence.db.checkpoint.SplitAndSortCpPagesTest.getCpPagesCntFromPagesMulticollection;
import static org.apache.ignite.internal.processors.cache.persistence.db.checkpoint.SplitAndSortCpPagesTest.getTestCollection;
import static org.apache.ignite.internal.processors.cache.persistence.db.checkpoint.SplitAndSortCpPagesTest.validateOrder;
import static org.junit.Assert.assertEquals;

public class AsyncCpSplitCpPagesUnitTest {

    private static final ForkJoinPool pool = new ForkJoinPool();

    @AfterClass
    public static void close() {
        pool.shutdown();
        try {
            if(!pool.awaitTermination(1, TimeUnit.SECONDS))
                pool.shutdownNow();
        }
        catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testAsyncLazyCpPagesSubmit() throws ExecutionException, InterruptedException, IgniteCheckedException {
        Collection<GridMultiCollectionWrapper<FullPageId>> coll = getTestCollection();
        int size = getCpPagesCntFromPagesMulticollection(coll);

        final AsyncCheckpointer asyncCheckpointer = new AsyncCheckpointer(6, getClass().getSimpleName(), new NullLogger());

        final BlockingQueue<FullPageId[]> queue = asyncCheckpointer != null ? new LinkedBlockingQueue<FullPageId[]>() : null;
        final ForkJoinTask<Integer> task
            = asyncCheckpointer.splitAndSortCpPagesIfNeeded3(new T2<>(coll, size), queue);
        final AtomicInteger totalPagesAfterSort = new AtomicInteger();
        final CountDownFuture fut = asyncCheckpointer.lazySubmit(task, queue,
            new IgniteBiClosure<FullPageId[], CountDownFuture, Runnable>() {
            @Override public Runnable apply(final FullPageId[] ids, final CountDownFuture future) {
                return new Runnable() {
                    @Override public void run() {
                        final int length = ids.length;
                        totalPagesAfterSort.addAndGet(length);
                        validateOrder(Arrays.asList(ids), length);

                        future.onDone((Void)null);

                    }
                };
            }
        });

        fut.get();
        assertEquals(totalPagesAfterSort.get(), size);
    }
}
