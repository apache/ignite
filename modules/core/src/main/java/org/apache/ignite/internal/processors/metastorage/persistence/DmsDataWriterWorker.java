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
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Consumer;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.processors.cache.persistence.metastorage.ReadWriteMetastorage;
import org.apache.ignite.internal.util.worker.GridWorker;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

import static org.apache.ignite.internal.processors.metastorage.persistence.DistributedMetaStorageUtil.COMMON_KEY_PREFIX;
import static org.apache.ignite.internal.processors.metastorage.persistence.DistributedMetaStorageUtil.cleanupGuardKey;
import static org.apache.ignite.internal.processors.metastorage.persistence.DistributedMetaStorageUtil.historyItemKey;
import static org.apache.ignite.internal.processors.metastorage.persistence.DistributedMetaStorageUtil.localKey;
import static org.apache.ignite.internal.processors.metastorage.persistence.DistributedMetaStorageUtil.versionKey;
import static org.apache.ignite.internal.processors.metastorage.persistence.DmsWorkerStatus.CANCEL;
import static org.apache.ignite.internal.processors.metastorage.persistence.DmsWorkerStatus.CONTINUE;
import static org.apache.ignite.internal.processors.metastorage.persistence.DmsWorkerStatus.HALT;

/** */
class DmsDataWriterWorker extends GridWorker {
    /** */
    public static final byte[] DUMMY_VALUE = {};

    /** */
    private final LinkedBlockingQueue<Object> updateQueue = new LinkedBlockingQueue<>();

    /** */
    private final DmsLocalMetaStorageLock lock;

    /** */
    private final Consumer<Throwable> errorHnd;

    /** */
    @TestOnly
    public DmsWorkerStatus status() {
        return status;
    }

    /** */
    private volatile DmsWorkerStatus status = CONTINUE;

    /** */
    private DistributedMetaStorageVersion workerDmsVer;

    /** */
    private volatile ReadWriteMetastorage metastorage;

    /** */
    private volatile boolean firstStart = true;

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
    }

    /** */
    public void setMetaStorage(ReadWriteMetastorage metastorage) {
        this.metastorage = metastorage;
    }

    /** */
    public void update(DistributedMetaStorageHistoryItem histItem) {
        updateQueue.offer(histItem);
    }

    /** */
    public void update(DistributedMetaStorageClusterNodeData fullNodeData) {
        assert fullNodeData.fullData != null;
        assert fullNodeData.hist != null;

        updateQueue.clear();

        updateQueue.offer(fullNodeData);
    }

    /** */
    public void removeHistItem(long ver) {
        updateQueue.offer(ver);
    }

    /** */
    public void cancel(boolean halt) throws InterruptedException {
        if (halt)
            updateQueue.clear();

        updateQueue.offer(status = halt ? HALT : CANCEL);

        Thread runner = runner();

        if (runner != null)
            runner.join();
    }

    /** {@inheritDoc} */
    @Override protected void body() throws InterruptedException, IgniteInterruptedCheckedException {
        status = CONTINUE;

        try {
            if (firstStart) {
                firstStart = false;

                lock.lock();

                try {
                    restore();
                }
                finally {
                    lock.unlock();
                }
            }

            while (true) {
                Object update = updateQueue.peek();

                try {
                    update = updateQueue.take();
                }
                catch (InterruptedException ignore) {
                }

                lock.lock();

                try {
                    // process update
                    if (update instanceof DistributedMetaStorageHistoryItem)
                        applyUpdate((DistributedMetaStorageHistoryItem)update);
                    else if (update instanceof DistributedMetaStorageClusterNodeData) {
                        DistributedMetaStorageClusterNodeData fullNodeData = (DistributedMetaStorageClusterNodeData)update;

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
                    }
                    else if (update instanceof Long) {
                        long ver = (Long)update;

                        metastorage.remove(historyItemKey(ver));
                    }
                    else {
                        assert update instanceof DmsWorkerStatus : update;

                        break;
                    }
                }
                finally {
                    lock.unlock();
                }
            }
        }
        catch (Throwable t) {
            errorHnd.accept(t);
        }
    }

    /** */
    private void applyUpdate(DistributedMetaStorageHistoryItem histItem) throws IgniteCheckedException {
        metastorage.write(historyItemKey(workerDmsVer.id() + 1), histItem);

        workerDmsVer = workerDmsVer.nextVersion(histItem);

        metastorage.write(versionKey(), workerDmsVer);

        for (int i = 0, len = histItem.keys().length; i < len; i++)
            write(histItem.keys()[i], histItem.valuesBytesArray()[i]);
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
}
