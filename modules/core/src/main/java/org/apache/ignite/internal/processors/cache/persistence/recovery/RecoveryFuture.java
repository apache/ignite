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

package org.apache.ignite.internal.processors.cache.persistence.recovery;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.pagemem.wal.WALIterator;
import org.apache.ignite.internal.pagemem.wal.WALPointer;
import org.apache.ignite.internal.pagemem.wal.record.DataEntry;
import org.apache.ignite.internal.pagemem.wal.record.DataRecord;
import org.apache.ignite.internal.pagemem.wal.record.TxRecord;
import org.apache.ignite.internal.pagemem.wal.record.WALRecord;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.GridConcurrentHashSet;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.typedef.CI1;
import org.apache.ignite.internal.util.typedef.CI2;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.jetbrains.annotations.Nullable;

import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.WRITE;
import static java.util.EnumSet.of;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_DEBUG_ON_RECOVERY;
import static org.apache.ignite.IgniteSystemProperties.getBoolean;

/**
 *
 */
public class RecoveryFuture extends GridFutureAdapter<RecoveryContext>
    implements IgniteRecoveryFuture<RecoveryContext> {
    /** */
    public static final long DOUBLE_CHECK_INTERVAL = 10_000;

    /** */
    private static final boolean DEBUG_ON_RECOVERY = getBoolean(IGNITE_DEBUG_ON_RECOVERY, false);

    /** */
    private final IgniteLogger log;

    /** */
    private final RecoveryIo recoveryIo;

    /** */
    private final GridFutureAdapter initFut = new GridFutureAdapter();

    /** */
    private final TxWalState txWalState = new TxWalState();

    /** */
    private final Set<GridCacheVersion> skipTxs = new HashSet<>();

    /** */
    private final Set<String> remaining = new GridConcurrentHashSet<>();

    /** */
    private WALPointer initPtr;

    /** */
    private volatile String localNodeConstId;

    /** */
    private Set<GridCacheVersion> notFoundTxs = new HashSet<>();

    /** */
    private final boolean debugEnable;

    /** */
    private RecoveryDebug debugBuffer;

    /**
     *
     */
    public RecoveryFuture(final RecoveryIo recoveryIo, @Nullable final IgniteLogger log) {
        this(recoveryIo, log, DEBUG_ON_RECOVERY);
    }

    /**
     *
     */
    public RecoveryFuture(
        final RecoveryIo recoveryIo,
        @Nullable final IgniteLogger log,
        boolean debugEnable
    ) {
        this.debugEnable = debugEnable;
        this.log = log;
        this.recoveryIo = recoveryIo;

        recoveryIo.receive(new CI2<String, Message>() {
            @Override public void apply(final String constId, final Message msg) {
                initFut.listen(new CI1<IgniteInternalFuture>() {
                    @Override public void apply(IgniteInternalFuture f) {
                        try {
                            f.get();

                            if (msg instanceof TxStateRequest) {
                                if (log.isDebugEnabled())
                                    log.debug("Receive tx request [local - " + localNodeConstId + " remote - " + constId + ']');

                                TxStateRequest req = (TxStateRequest)msg;

                                TxStateResponse res = new TxStateResponse();

                                for (GridCacheVersion txVer : req.prepared) {
                                    boolean preparing = txWalState.isPreparing(txVer);
                                    boolean rollBacked = txWalState.isRollBacked(txVer);

                                    boolean commited = txWalState.isCommited(txVer);
                                    boolean prepared = txWalState.isPrepared(txVer);

                                    if (preparing || rollBacked)
                                        res.rollBackTxs.add(txVer);
                                    else if (!commited && !prepared) {
                                        //todo check as not found, need re-scan in past.
                                        System.err.println("NOT FOUND " + txVer + " localNode " + localNodeConstId);

                                        notFoundTxs.add(txVer);

                                        res.rollBackTxs.add(txVer);
                                    }
                                }

                                sendResponse(constId, res);
                            }
                            else if (msg instanceof TxStateResponse) {
                                TxStateResponse res = (TxStateResponse)msg;

                                if (log.isDebugEnabled())
                                    log.debug("Receive tx response [local - " + localNodeConstId + " remote - " + constId + ']');

                                boolean allReceived;

                                synchronized (this) {
                                    skipTxs.addAll(res.rollBackTxs);

                                    remaining.remove(constId);

                                    allReceived = remaining.isEmpty();
                                }

                                if (allReceived)
                                    onDone(new RecoveryContext(initPtr, skipTxs));
                            }
                        }
                        catch (Throwable e) {
                            U.error(log, "Fail process recovery message.", e);

                            new Thread(new Runnable() {
                                @Override public void run() {
                                    G.stop(true);
                                }
                            }).start();
                        }
                    }
                });
            }
        });

        recoveryIo.onNodeLeft(new CI1<String>() {
            @Override public void apply(String constId) {
                boolean allReceived;

                synchronized (this) {
                    remaining.remove(constId);

                    allReceived = remaining.isEmpty();
                }

                if (allReceived)
                    onDone(new RecoveryContext(initPtr, skipTxs));
            }
        });
    }

    /**
     *
     */
    public synchronized void setRecoveryWalPoint(WALPointer initPtr) throws IgniteCheckedException {
        try {
            setRecoveryWalPoint(initPtr, U.defaultWorkDirectory());
        }
        catch (IgniteCheckedException e) {
            U.error(log, "Fail resolve work directory.", e);

            setRecoveryWalPoint(initPtr, null);
        }
    }

    /**
     *
     */
    public synchronized void setRecoveryWalPoint(WALPointer initPtr, String path) throws IgniteCheckedException {
        assert !initFut.isDone();

        this.initPtr = initPtr;
        this.localNodeConstId = recoveryIo.localNodeConsistentId();

        if (debugEnable) {
            if (path != null){
                String recoveryDumpFilePath = path + "/recovery-log-" + U.currentTimeMillis() + "-" + localNodeConstId + ".log";

                debugBuffer = new RecoveryDebug(recoveryDumpFilePath, log);
            }
            else
                debugBuffer = new RecoveryDebug();
        }
    }

    /** {@inheritDoc} */
    @Override public void recoveryScan(WALIterator it) {
        if (debugEnable)
            debugBuffer.append("Node constId:")
                .append(localNodeConstId)
                .append("\n");

        while (it.hasNext()) {
            if (isCancelled())
                break;

            IgniteBiTuple<WALPointer, WALRecord> tup = it.next();

            WALRecord rec = tup.get2();

            switch (rec.type()) {
                case TX_RECORD:
                    TxRecord txRec = (TxRecord)rec;

                    if (debugEnable)
                        debugBuffer.append(rec + "\n");

                    switch (txRec.state()) {
                        case PREPARING:
                            txWalState.onPreparing(txRec);

                            break;

                        case PREPARED:
                            txWalState.onPrepared(txRec);

                            break;
                        case COMMITTED:
                            txWalState.onCommited(txRec);

                            break;
                        case ROLLED_BACK:
                            txWalState.onRollbacked(txRec);

                            break;
                        default:
                            // Skip other types.
                    }

                    break;

                case CHECKPOINT_RECORD:
                    if (debugEnable)
                        debugBuffer.append(rec).append("\n");
                    break;

                case DATA_RECORD:
                    if (debugEnable) {
                        DataRecord dataRec = (DataRecord)rec;

                        debugBuffer.append(dataRec).append("\n");

                        for (DataEntry e : dataRec.writeEntries())
                            debugBuffer.append("\t").append(e).append("\n");
                    }

                    break;
                default:
                    // Skip other types.
            }
        }

        if (isCancelled())
            return;

        if (debugEnable)
            txWalState.appendDebugInfo(debugBuffer);

        skipTxs.addAll(txWalState.preparingTxs());
        skipTxs.addAll(txWalState.rollBackedTxs());

        Map<String, TxStateRequest> reqs = generateRequests();

        //Todo need to add only available node.
        remaining.addAll(reqs.keySet());

        initFut.onDone();

        if (remaining.isEmpty()) {
            onDone(new RecoveryContext(initPtr, skipTxs));

            return;
        }

        sentRequests(reqs);
    }

    /**
     *
     */
    private void sendResponse(String constId, TxStateResponse res) {
        try {
            recoveryIo.send(constId, res);

            if (log.isDebugEnabled())
                log.debug("Send tx response [" + localNodeConstId + " -> " + constId + ']');
        }
        catch (IgniteCheckedException e) {
            U.error(log, "Fail send message to " + constId, e);
        }
    }

    /**
     *
     */
    private void sentRequests(Map<String, TxStateRequest> prepared) {
        Iterator<String> it = remaining.iterator();

        while (it.hasNext()) {
            String constId = it.next();

            TxStateRequest req = prepared.get(constId);

            if (req == null)
                req = new TxStateRequest();

            try {
                recoveryIo.send(constId, req);

                if (log.isDebugEnabled())
                    log.debug("Send tx request [" + localNodeConstId + " -> " + constId + ']');
            }
            catch (IgniteCheckedException e) {
                U.error(log, "Fail send message to " + constId, e);

                synchronized (this) {
                    it.remove();
                }
            }
        }
    }

    /**
     *
     */
    private Map<String, TxStateRequest> generateRequests() {
        assert localNodeConstId != null : "Local node consistent id is not setup";

        Map<String, TxStateRequest> requestsMap = new HashMap<>();

        for (Map.Entry<GridCacheVersion, Set<String>> en : txWalState.preparedTxs().entrySet()) {
            GridCacheVersion txVer = en.getKey();

            for (String constId : en.getValue()) {
                if (localNodeConstId.equals(constId))
                    continue;

                TxStateRequest req = requestsMap.get(constId);

                if (req == null)
                    requestsMap.put(constId, req = new TxStateRequest());

                req.prepared.add(txVer);
            }
        }

        return requestsMap;
    }

    /** {@inheritDoc} */
    @Override public boolean onDone(
        @Nullable RecoveryContext res,
        @Nullable Throwable err
    ) {
        if (!isDone() && log != null && log.isInfoEnabled()) {
            String debugLog = null;

            if (debugEnable) {
                debugBuffer.append("skipTxs ")
                    .append(res.getSkipTxEntries().size())
                    .append("\n");

                for (GridCacheVersion txVer : res.getSkipTxEntries())
                    debugBuffer.append(txVer).append("\n");

                debugLog = debugBuffer.toString();

                debugBuffer.close();
            }

            log.info("Recovery scan future is done constId:" + localNodeConstId +
                ", init pointer " + res.getInitPnt() + " tx will be rollBacked (" + res.getSkipTxEntries().size() +
                ") \n" + (debugLog != null ? debugLog : "") +
                "(constId:" + localNodeConstId + ")\n");
        }

        return super.onDone(res, err);
    }

    static class RecoveryDebug {
        /** */
        private final StringBuilder debugBuffer = new StringBuilder();

        /** */
        private FileChannel fc;

        private final IgniteLogger log;

        RecoveryDebug() {
            // No-op.
            log = null;
        }

        RecoveryDebug(String path, @Nullable IgniteLogger log) {
            this.log = log;

            File f = new File(path);

            if (f.exists())
                f.delete();

            try {
                f.createNewFile();

                fc = FileChannel.open(Paths.get(f.getAbsolutePath()), of(READ, WRITE));
            }
            catch (IOException e) {
                U.error(log, "Fail create recovery dump file.", e);

                fc = null;
            }
        }

        public RecoveryDebug append(Object st) {
            debugBuffer.append(st);

            if (fc != null)
                appendFile(st);

            return this;
        }

        private void appendFile(Object st) {
            if (fc != null)
                try {
                    fc.write(ByteBuffer.wrap(st.toString().getBytes()));
                }
                catch (IOException e) {
                    U.error(log, "Fail write to recovery dump file.", e);
                }
        }

        @Override public String toString() {
            return debugBuffer.toString();
        }

        private void close() {
            if (fc != null)
                try {
                    fc.force(true);

                    fc.close();
                }
                catch (IOException e) {
                    U.error(log, "Fail close recovery dump file.", e);
                }
        }
    }
}
