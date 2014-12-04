/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.loadtests.hashmap;

import org.gridgain.grid.*;
import org.gridgain.grid.util.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.loadtests.util.*;
import org.jdk8.backport.*;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import static org.jdk8.backport.ConcurrentLinkedHashMap.QueuePolicy;
import static org.jdk8.backport.ConcurrentLinkedHashMap.QueuePolicy.*;

/**
 *
 */
public class GridBoundedConcurrentLinkedHashSetLoadTest {
    /** */
    public static final int UPDATE_INTERVAL_SEC = 5;

    /**
     * @param args Arguments.
     */
    public static void main(String[] args) throws Exception {
        QueuePolicy qPlc = args.length > 0 ? QueuePolicy.valueOf(args[0]) : SINGLE_Q;
        int threadCnt = args.length > 1 ? Integer.valueOf(args[1]) : Runtime.getRuntime().availableProcessors();

        X.println("Queue policy: " + qPlc);
        X.println("Threads: " + threadCnt);

        ExecutorService pool = Executors.newFixedThreadPool(threadCnt);

        final Collection<IgniteUuid> set =
            new GridBoundedConcurrentLinkedHashSet<>(10240, 32, 0.75f, 128, qPlc);

        X.println("Set: " + set);

        final LongAdder execCnt = new LongAdder();

        final AtomicBoolean finish = new AtomicBoolean();

        // Thread that measures and outputs performance statistics.
        Thread collector = new Thread(new Runnable() {
            @SuppressWarnings({"BusyWait", "InfiniteLoopStatement"})
            @Override public void run() {
                GridCumulativeAverage avgTasksPerSec = new GridCumulativeAverage();

                try {
                    while (!finish.get()) {
                        Thread.sleep(UPDATE_INTERVAL_SEC * 1000);

                        long curTasksPerSec = execCnt.sumThenReset() / UPDATE_INTERVAL_SEC;

                        X.println(">>> Tasks/s: " + curTasksPerSec);

                        avgTasksPerSec.update(curTasksPerSec);
                    }
                }
                catch (InterruptedException ignored) {
                    X.println(">>> Interrupted.");

                    Thread.currentThread().interrupt();
                }
            }
        });

        collector.start();

        Collection<Callable<Object>> producers = new ArrayList<>(threadCnt);

        for (int i = 0; i < threadCnt; i++)
            producers.add(new Callable<Object>() {
                @SuppressWarnings({"unchecked", "InfiniteLoopStatement"})
                @Override public Object call() throws Exception {
                    UUID id = UUID.randomUUID();

                    try {
                    while (!finish.get()) {
                        set.add(IgniteUuid.fromUuid(id));

                        execCnt.increment();
                    }

                    return null;
                    }
                    catch (Throwable t) {
                        t.printStackTrace();

                        throw new Exception(t);
                    }
                    finally {
                        X.println("Thread finished.");
                    }
                }
            });

        pool.invokeAll(producers);
    }
}
