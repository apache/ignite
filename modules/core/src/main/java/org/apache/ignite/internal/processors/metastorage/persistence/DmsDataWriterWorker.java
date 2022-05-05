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

package org.apache.ignite.internal.processors.metastorage.persistence;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RunnableFuture;
import java.util.function.Consumer;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.cache.persistence.metastorage.ReadWriteMetastorage;
import org.apache.ignite.internal.util.lang.IgniteThrowableRunner;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.util.worker.GridWorker;
import org.apache.ignite.thread.IgniteThread;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.processors.metastorage.persistence.DistributedMetaStorageUtil.COMMON_KEY_PREFIX;
import static org.apache.ignite.internal.processors.metastorage.persistence.DistributedMetaStorageUtil.cleanupGuardKey;
import static org.apache.ignite.internal.processors.metastorage.persistence.DistributedMetaStorageUtil.historyItemKey;
import static org.apache.ignite.internal.processors.metastorage.persistence.DistributedMetaStorageUtil.localKey;
import static org.apache.ignite.internal.processors.metastorage.persistence.DistributedMetaStorageUtil.versionKey;

/** */
public class DmsDataWriterWorker extends GridWorker {
    /** */
    public static final byte[] DUMMY_VALUE = {};

    /** */
    private static final Object STOP = new Object();

    /** */
    private static final Object AWAIT = new Object();

    /** */
    private final LinkedBlockingQueue<RunnableFuture<?>> updateQueue = new LinkedBlockingQueue<>();

    /** */
    private final DmsLocalMetaStorageLock lock;

    /** */
    private final Consumer<Throwable> errorHnd;

    /** */
    private DistributedMetaStorageVersion workerDmsVer;

    /** */
    private volatile ReadWriteMetastorage metastorage;

    /** */
    private volatile CountDownLatch latch = new CountDownLatch(0);

    /**
     * This task is used to pause processing of the {@code updateQueue}. If this task completed it means that all the updates
     * prior to it already flushed to the local metastorage.
     */
    private volatile Future<?> suspendFut = CompletableFuture.completedFuture(AWAIT);

    /** */
    public DmsDataWriterWorker(
        @Nullable String igniteInstanceName,
        IgniteLogger log,
        DmsLocalMetaStorageLock lock,
        Consumer<Throwable> errorHnd
    ) {
        super(igniteInstanceName, "dms-writer", log);
        this.lock = lock;
        this.errorHnd = errorHnd;

        // Put restore task to the queue, so it will be executed on worker start.
        updateQueue.offer(newDmsTask(this::restore));
    }

    /** */
    public void setMetaStorage(ReadWriteMetastorage metastorage) {
        this.metastorage = metastorage;
    }

    /** Start new distributed metastorage worker thread. */
    public void start() {
        isCancelled = false;

        new IgniteThread(igniteInstanceName(), "dms-writer-thread", this).start();
    }

    /**
     * @return Future which will be completed when all tasks prior to the pause task are finished.
     */
    public Future<?> flush() {
        return suspendFut;
    }

    /**
     * @param compFut Future which should be completed when worker may proceed with updates.
     */
    public void suspend(IgniteInternalFuture<?> compFut) {
        if (isCancelled())
            suspendFut = CompletableFuture.completedFuture(AWAIT);
        else {
            latch = new CountDownLatch(1);

            updateQueue.offer((RunnableFuture<?>)(suspendFut = new FutureTask<>(() -> AWAIT)));

            compFut.listen(f -> latch.countDown());
        }
    }

    /** */
    public void update(DistributedMetaStorageHistoryItem histItem) {
        updateQueue.offer(newDmsTask(() -> {
            metastorage.write(historyItemKey(workerDmsVer.id() + 1), histItem);

            workerDmsVer = workerDmsVer.nextVersion(histItem);

            metastorage.write(versionKey(), workerDmsVer);

            for (int i = 0, len = histItem.keys().length; i < len; i++)
                write(histItem.keys()[i], histItem.valuesBytesArray()[i]);
        }));
    }

    /** */
    public void update(DistributedMetaStorageClusterNodeData fullNodeData) {
        assert fullNodeData.fullData != null;
        assert fullNodeData.hist != null;

        updateQueue.offer(newDmsTask(() -> {
            metastorage.writeRaw(cleanupGuardKey(), DUMMY_VALUE);

            doCleanup();

            for (DistributedMetaStorageKeyValuePair item : fullNodeData.fullData)
                metastorage.writeRaw(localKey(item.key), item.valBytes);

            for (int i = 0, len = fullNodeData.hist.length; i < len; i++) {
                DistributedMetaStorageHistoryItem histItem = fullNodeData.hist[i];

                long histItemVer = fullNodeData.ver.id() + i - (len - 1);

                metastorage.write(historyItemKey(histItemVer), histItem);
            }

            metastorage.write(versionKey(), fullNodeData.ver);

            workerDmsVer = fullNodeData.ver;

            metastorage.remove(cleanupGuardKey());
        }));
    }

    /** */
    public void removeHistItem(long ver) {
        updateQueue.offer(newDmsTask(() -> metastorage.remove(historyItemKey(ver))));
    }

    /** */
    public void cancel(boolean halt) throws InterruptedException {
        if (halt) {
            updateQueue.clear();

            if (suspendFut instanceof RunnableFuture)
                ((Runnable)suspendFut).run();
        }

        updateQueue.offer(new FutureTask<>(() -> STOP));
        latch.countDown();

        isCancelled = true;

        Thread runner = runner();

        if (runner != null)
            runner.join();
    }

    /** {@inheritDoc} */
    @Override protected void body() {
        while (true) {
            try {
                RunnableFuture<?> curTask = updateQueue.take();

                curTask.run();

                // Result will be null for any runnable executed tasks over metastorage and non-null for system DMS tasks.
                Object res = U.get(curTask);

                if (res == STOP)
                    break;

                if (res == AWAIT)
                    latch.await();
            }
            catch (InterruptedException ignore) {
            }
            catch (Throwable t) {
                errorHnd.accept(t);

                break;
            }
        }
    }

    /** */
    private void restore() throws IgniteCheckedException {
        if (metastorage.readRaw(cleanupGuardKey()) != null) {
            doCleanup();

            metastorage.remove(cleanupGuardKey());
        }
        else {
            DistributedMetaStorageVersion storedVer =
                (DistributedMetaStorageVersion)metastorage.read(versionKey());

            if (storedVer == null) {
                workerDmsVer = DistributedMetaStorageVersion.INITIAL_VERSION;

                metastorage.write(versionKey(), DistributedMetaStorageVersion.INITIAL_VERSION);
            }
            else {
                DistributedMetaStorageHistoryItem histItem =
                    (DistributedMetaStorageHistoryItem)metastorage.read(historyItemKey(storedVer.id() + 1));

                if (histItem != null) {
                    workerDmsVer = storedVer.nextVersion(histItem);

                    metastorage.write(versionKey(), workerDmsVer);

                    for (int i = 0, len = histItem.keys().length; i < len; i++)
                        write(histItem.keys()[i], histItem.valuesBytesArray()[i]);
                }
                else {
                    workerDmsVer = storedVer;

                    histItem = (DistributedMetaStorageHistoryItem)metastorage.read(historyItemKey(storedVer.id()));

                    if (histItem != null) {
                        boolean equal = true;

                        for (int i = 0, len = histItem.keys().length; i < len; i++) {
                            byte[] valBytes = metastorage.readRaw(localKey(histItem.keys()[i]));

                            if (!equal || !Arrays.equals(valBytes, histItem.valuesBytesArray()[i])) {
                                equal = false;

                                write(histItem.keys()[i], histItem.valuesBytesArray()[i]);
                            }
                        }
                    }
                }
            }
        }
    }

    /** */
    private void doCleanup() throws IgniteCheckedException {
        Set<String> allKeys = new HashSet<>();

        metastorage.iterate(COMMON_KEY_PREFIX, (key, val) -> allKeys.add(key), false);

        allKeys.remove(cleanupGuardKey());

        for (String key : allKeys)
            metastorage.remove(key);

        workerDmsVer = DistributedMetaStorageVersion.INITIAL_VERSION;

        metastorage.write(versionKey(), DistributedMetaStorageVersion.INITIAL_VERSION);
    }

    /** */
    private void write(String key, byte[] valBytes) throws IgniteCheckedException {
        if (valBytes == null)
            metastorage.remove(localKey(key));
        else
            metastorage.writeRaw(localKey(key), valBytes);
    }

    /**
     * @param task Task to execute on local metastorage.
     * @return Future will be completed when task has been finished.
     */
    private RunnableFuture<Void> newDmsTask(IgniteThrowableRunner task) {
        return new FutureTask<>(() -> {
            lock.lock();

            try {
                task.run();
            }
            catch (IgniteCheckedException e) {
                throw U.convertException(e);
            }
            finally {
                lock.unlock();
            }
        }, null);
    }
}
