/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ignite.raft.jraft.storage.snapshot;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.ignite.raft.jraft.Closure;
import org.apache.ignite.raft.jraft.FSMCaller;
import org.apache.ignite.raft.jraft.Status;
import org.apache.ignite.raft.jraft.closure.LoadSnapshotClosure;
import org.apache.ignite.raft.jraft.closure.SaveSnapshotClosure;
import org.apache.ignite.raft.jraft.core.NodeImpl;
import org.apache.ignite.raft.jraft.entity.EnumOutter.ErrorType;
import org.apache.ignite.raft.jraft.entity.RaftOutter.SnapshotMeta;
import org.apache.ignite.raft.jraft.error.RaftError;
import org.apache.ignite.raft.jraft.error.RaftException;
import org.apache.ignite.raft.jraft.option.SnapshotCopierOptions;
import org.apache.ignite.raft.jraft.option.SnapshotExecutorOptions;
import org.apache.ignite.raft.jraft.rpc.RaftRpcFactory;
import org.apache.ignite.raft.jraft.rpc.RpcRequestClosure;
import org.apache.ignite.raft.jraft.rpc.RpcRequests.InstallSnapshotRequest;
import org.apache.ignite.raft.jraft.rpc.RpcRequests.InstallSnapshotResponse;
import org.apache.ignite.raft.jraft.storage.LogManager;
import org.apache.ignite.raft.jraft.storage.SnapshotExecutor;
import org.apache.ignite.raft.jraft.storage.SnapshotStorage;
import org.apache.ignite.raft.jraft.storage.snapshot.local.LocalSnapshotStorage;
import org.apache.ignite.raft.jraft.util.CountDownEvent;
import org.apache.ignite.raft.jraft.util.OnlyForTest;
import org.apache.ignite.raft.jraft.util.Requires;
import org.apache.ignite.raft.jraft.util.StringUtils;
import org.apache.ignite.raft.jraft.util.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Snapshot executor implementation.
 */
public class SnapshotExecutorImpl implements SnapshotExecutor {
    private static final Logger LOG = LoggerFactory.getLogger(SnapshotExecutorImpl.class);

    private final Lock lock = new ReentrantLock();

    private long lastSnapshotTerm;
    private long lastSnapshotIndex;
    private long term;
    private volatile boolean savingSnapshot;
    private volatile boolean loadingSnapshot;
    private volatile boolean stopped;
    private SnapshotStorage snapshotStorage;
    private SnapshotCopier curCopier;
    private FSMCaller fsmCaller;
    private NodeImpl node;
    private LogManager logManager;
    private final AtomicReference<DownloadingSnapshot> downloadingSnapshot = new AtomicReference<>(null);
    private SnapshotMeta loadingSnapshotMeta;
    private final CountDownEvent runningJobs = new CountDownEvent();

    /**
     * Downloading snapshot job.
     */
    static class DownloadingSnapshot {
        InstallSnapshotRequest request;
        InstallSnapshotResponse.Builder responseBuilder;
        RpcRequestClosure done;

        DownloadingSnapshot(final InstallSnapshotRequest request,
            final InstallSnapshotResponse.Builder responseBuilder, final RpcRequestClosure done) {
            super();
            this.request = request;
            this.responseBuilder = responseBuilder;
            this.done = done;
        }
    }

    /**
     * Only for test
     */
    @OnlyForTest
    public long getLastSnapshotTerm() {
        return this.lastSnapshotTerm;
    }

    /**
     * Only for test
     */
    @OnlyForTest
    public long getLastSnapshotIndex() {
        return this.lastSnapshotIndex;
    }

    /**
     * Save snapshot done closure
     */
    private class SaveSnapshotDone implements SaveSnapshotClosure {

        SnapshotWriter writer;
        Closure done;
        SnapshotMeta meta;
        Executor executor;

        SaveSnapshotDone(final SnapshotWriter writer, final Closure done, final SnapshotMeta meta,
            Executor executor) {
            super();
            this.writer = writer;
            this.done = done;
            this.meta = meta;
            this.executor = executor;
        }

        @Override
        public void run(final Status status) {
            Utils.runInThread(executor, () -> continueRun(status));
        }

        void continueRun(final Status st) {
            final int ret = onSnapshotSaveDone(st, this.meta, this.writer);
            if (ret != 0 && st.isOk()) {
                st.setError(ret, "node call onSnapshotSaveDone failed");
            }
            if (this.done != null) {
                Utils.runClosureInExecutor(executor, this.done, st);
            }
        }

        @Override
        public SnapshotWriter start(final SnapshotMeta meta) {
            this.meta = meta;
            return this.writer;
        }
    }

    /**
     * Install snapshot done closure
     */
    private class InstallSnapshotDone implements LoadSnapshotClosure {

        SnapshotReader reader;

        InstallSnapshotDone(final SnapshotReader reader) {
            super();
            this.reader = reader;
        }

        @Override
        public void run(final Status status) {
            onSnapshotLoadDone(status);
        }

        @Override
        public SnapshotReader start() {
            return this.reader;
        }
    }

    /**
     * Load snapshot at first time closure
     */
    private class FirstSnapshotLoadDone implements LoadSnapshotClosure {

        SnapshotReader reader;
        CountDownLatch eventLatch;
        Status status;

        FirstSnapshotLoadDone(final SnapshotReader reader) {
            super();
            this.reader = reader;
            this.eventLatch = new CountDownLatch(1);
        }

        @Override
        public void run(final Status status) {
            this.status = status;
            onSnapshotLoadDone(this.status);
            this.eventLatch.countDown();
        }

        public void waitForRun() throws InterruptedException {
            this.eventLatch.await();
        }

        @Override
        public SnapshotReader start() {
            return this.reader;
        }

    }

    @Override
    public boolean init(final SnapshotExecutorOptions opts) {
        if (StringUtils.isBlank(opts.getUri())) {
            LOG.error("Snapshot uri is empty.");
            return false;
        }
        this.logManager = opts.getLogManager();
        this.fsmCaller = opts.getFsmCaller();
        this.node = opts.getNode();
        this.term = opts.getInitTerm();
        this.snapshotStorage = this.node.getServiceFactory().createSnapshotStorage(opts.getUri(),
            this.node.getRaftOptions());
        if (opts.isFilterBeforeCopyRemote()) {
            this.snapshotStorage.setFilterBeforeCopyRemote();
        }
        if (opts.getSnapshotThrottle() != null) {
            this.snapshotStorage.setSnapshotThrottle(opts.getSnapshotThrottle());
        }
        if (!this.snapshotStorage.init(null)) {
            LOG.error("Fail to init snapshot storage.");
            return false;
        }
        final LocalSnapshotStorage tmp = (LocalSnapshotStorage) this.snapshotStorage;
        if (tmp != null && !tmp.hasServerAddr()) {
            tmp.setServerAddr(opts.getAddr());
        }
        final SnapshotReader reader = this.snapshotStorage.open();
        if (reader == null) {
            return true;
        }
        this.loadingSnapshotMeta = reader.load();
        if (this.loadingSnapshotMeta == null) {
            LOG.error("Fail to load meta from {}.", opts.getUri());
            Utils.closeQuietly(reader);
            return false;
        }
        LOG.info("Loading snapshot, meta={}.", this.loadingSnapshotMeta);
        this.loadingSnapshot = true;
        this.runningJobs.incrementAndGet();
        final FirstSnapshotLoadDone done = new FirstSnapshotLoadDone(reader);
        Requires.requireTrue(this.fsmCaller.onSnapshotLoad(done));
        try {
            done.waitForRun();
        }
        catch (final InterruptedException e) {
            LOG.warn("Wait for FirstSnapshotLoadDone run is interrupted.");
            Thread.currentThread().interrupt();
            return false;
        }
        finally {
            Utils.closeQuietly(reader);
        }
        if (!done.status.isOk()) {
            LOG.error("Fail to load snapshot from {}, FirstSnapshotLoadDone status is {}.", opts.getUri(), done.status);
            return false;
        }
        return true;
    }

    @Override
    public void shutdown() {
        long savedTerm;
        this.lock.lock();
        try {
            savedTerm = this.term;
            this.stopped = true;
        }
        finally {
            this.lock.unlock();
        }
        interruptDownloadingSnapshots(savedTerm);
    }

    @Override
    public NodeImpl getNode() {
        return this.node;
    }

    @Override
    public void doSnapshot(final Closure done) {
        boolean doUnlock = true;
        this.lock.lock();
        try {
            if (this.stopped) {
                Utils.runClosureInThread(this.node.getOptions().getCommonExecutor(), done, new Status(RaftError.EPERM, "Is stopped."));
                return;
            }
            if (this.downloadingSnapshot.get() != null) {
                Utils.runClosureInThread(this.node.getOptions().getCommonExecutor(), done, new Status(RaftError.EBUSY, "Is loading another snapshot."));
                return;
            }

            if (this.savingSnapshot) {
                Utils.runClosureInThread(this.node.getOptions().getCommonExecutor(), done, new Status(RaftError.EBUSY, "Is saving another snapshot."));
                return;
            }

            if (this.fsmCaller.getLastAppliedIndex() == this.lastSnapshotIndex) {
                // There might be false positive as the getLastAppliedIndex() is being
                // updated. But it's fine since we will do next snapshot saving in a
                // predictable time.
                doUnlock = false;
                this.lock.unlock();
                this.logManager.clearBufferedLogs();
                Utils.runClosureInThread(this.node.getOptions().getCommonExecutor(), done);
                return;
            }

            final long distance = this.fsmCaller.getLastAppliedIndex() - this.lastSnapshotIndex;
            if (distance < this.node.getOptions().getSnapshotLogIndexMargin()) {
                // If state machine's lastAppliedIndex value minus lastSnapshotIndex value is
                // less than snapshotLogIndexMargin value, then directly return.
                if (this.node != null) {
                    LOG.debug(
                        "Node {} snapshotLogIndexMargin={}, distance={}, so ignore this time of snapshot by snapshotLogIndexMargin setting.",
                        this.node.getNodeId(), distance, this.node.getOptions().getSnapshotLogIndexMargin());
                }
                doUnlock = false;
                this.lock.unlock();
                Utils.runClosureInThread(this.node.getOptions().getCommonExecutor(), done);
                return;
            }

            final SnapshotWriter writer = this.snapshotStorage.create();
            if (writer == null) {
                Utils.runClosureInThread(this.node.getOptions().getCommonExecutor(), done, new Status(RaftError.EIO, "Fail to create writer."));
                reportError(RaftError.EIO.getNumber(), "Fail to create snapshot writer.");
                return;
            }
            this.savingSnapshot = true;
            final SaveSnapshotDone saveSnapshotDone = new SaveSnapshotDone(writer, done, null, this.node.getOptions().getCommonExecutor());
            if (!this.fsmCaller.onSnapshotSave(saveSnapshotDone)) {
                Utils.runClosureInThread(this.node.getOptions().getCommonExecutor(), done, new Status(RaftError.EHOSTDOWN, "The raft node is down."));
                return;
            }
            this.runningJobs.incrementAndGet();
        }
        finally {
            if (doUnlock) {
                this.lock.unlock();
            }
        }

    }

    int onSnapshotSaveDone(final Status st, final SnapshotMeta meta, final SnapshotWriter writer) {
        int ret;
        this.lock.lock();
        try {
            ret = st.getCode();
            // InstallSnapshot can break SaveSnapshot, check InstallSnapshot when SaveSnapshot
            // because upstream Snapshot maybe newer than local Snapshot.
            if (st.isOk()) {
                if (meta.getLastIncludedIndex() <= this.lastSnapshotIndex) {
                    ret = RaftError.ESTALE.getNumber();
                    if (this.node != null) {
                        LOG.warn("Node {} discards an stale snapshot lastIncludedIndex={}, lastSnapshotIndex={}.",
                            this.node.getNodeId(), meta.getLastIncludedIndex(), this.lastSnapshotIndex);
                    }
                    writer.setError(RaftError.ESTALE, "Installing snapshot is older than local snapshot");
                }
            }
        }
        finally {
            this.lock.unlock();
        }

        if (ret == 0) {
            if (!writer.saveMeta(meta)) {
                LOG.warn("Fail to save snapshot {}.", writer.getPath());
                ret = RaftError.EIO.getNumber();
            }
        }
        else {
            if (writer.isOk()) {
                writer.setError(ret, "Fail to do snapshot.");
            }
        }
        try {
            writer.close();
        }
        catch (final IOException e) {
            LOG.error("Fail to close writer", e);
            ret = RaftError.EIO.getNumber();
        }
        boolean doUnlock = true;
        this.lock.lock();
        try {
            if (ret == 0) {
                this.lastSnapshotIndex = meta.getLastIncludedIndex();
                this.lastSnapshotTerm = meta.getLastIncludedTerm();
                doUnlock = false;
                this.lock.unlock();
                this.logManager.setSnapshot(meta); // should be out of lock
                doUnlock = true;
                this.lock.lock();
            }
            if (ret == RaftError.EIO.getNumber()) {
                reportError(RaftError.EIO.getNumber(), "Fail to save snapshot.");
            }
            this.savingSnapshot = false;
            this.runningJobs.countDown();
            return ret;

        }
        finally {
            if (doUnlock) {
                this.lock.unlock();
            }
        }
    }

    private void onSnapshotLoadDone(final Status st) {
        DownloadingSnapshot m;
        boolean doUnlock = true;
        this.lock.lock();
        try {
            Requires.requireTrue(this.loadingSnapshot, "Not loading snapshot");
            m = this.downloadingSnapshot.get();
            if (st.isOk()) {
                this.lastSnapshotIndex = this.loadingSnapshotMeta.getLastIncludedIndex();
                this.lastSnapshotTerm = this.loadingSnapshotMeta.getLastIncludedTerm();
                doUnlock = false;
                this.lock.unlock();
                this.logManager.setSnapshot(this.loadingSnapshotMeta); // should be out of lock
                doUnlock = true;
                this.lock.lock();
            }
            final StringBuilder sb = new StringBuilder();
            if (this.node != null) {
                sb.append("Node ").append(this.node.getNodeId()).append(" ");
            }
            sb.append("onSnapshotLoadDone, ").append(this.loadingSnapshotMeta);
            LOG.info(sb.toString());
            doUnlock = false;
            this.lock.unlock();
            if (this.node != null) {
                this.node.updateConfigurationAfterInstallingSnapshot();
            }
            doUnlock = true;
            this.lock.lock();
            this.loadingSnapshot = false;
            this.downloadingSnapshot.set(null);

        }
        finally {
            if (doUnlock) {
                this.lock.unlock();
            }
        }
        if (m != null) {
            // Respond RPC
            if (!st.isOk()) {
                m.done.run(st);
            }
            else {
                m.responseBuilder.setSuccess(true);
                m.done.sendResponse(m.responseBuilder.build());
            }
        }
        this.runningJobs.countDown();
    }

    @Override
    public void installSnapshot(final InstallSnapshotRequest request, final InstallSnapshotResponse.Builder response,
        final RpcRequestClosure done) {
        final SnapshotMeta meta = request.getMeta();
        final DownloadingSnapshot ds = new DownloadingSnapshot(request, response, done);
        // DON'T access request, response, and done after this point
        // as the retry snapshot will replace this one.
        if (!registerDownloadingSnapshot(ds)) {
            LOG.warn("Fail to register downloading snapshot.");
            // This RPC will be responded by the previous session
            return;
        }
        Requires.requireNonNull(this.curCopier, "curCopier");
        try {
            this.curCopier.join();
        }
        catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            LOG.warn("Install snapshot copy job was canceled.");
            return;
        }

        loadDownloadingSnapshot(ds, meta);
    }

    void loadDownloadingSnapshot(final DownloadingSnapshot ds, final SnapshotMeta meta) {
        SnapshotReader reader;
        this.lock.lock();
        try {
            if (ds != this.downloadingSnapshot.get()) {
                // It is interrupted and response by other request,just return
                return;
            }
            Requires.requireNonNull(this.curCopier, "curCopier");
            reader = this.curCopier.getReader();
            if (!this.curCopier.isOk()) {
                if (this.curCopier.getCode() == RaftError.EIO.getNumber()) {
                    reportError(this.curCopier.getCode(), this.curCopier.getErrorMsg());
                }
                Utils.closeQuietly(reader);
                ds.done.run(this.curCopier);
                Utils.closeQuietly(this.curCopier);
                this.curCopier = null;
                this.downloadingSnapshot.set(null);
                this.runningJobs.countDown();
                return;
            }
            Utils.closeQuietly(this.curCopier);
            this.curCopier = null;
            if (reader == null || !reader.isOk()) {
                Utils.closeQuietly(reader);
                this.downloadingSnapshot.set(null);
                ds.done.sendResponse(RaftRpcFactory.DEFAULT //
                    .newResponse(InstallSnapshotResponse.getDefaultInstance(), RaftError.EINTERNAL,
                        "Fail to copy snapshot from %s", ds.request.getUri()));
                this.runningJobs.countDown();
                return;
            }
            this.loadingSnapshot = true;
            this.loadingSnapshotMeta = meta;
        }
        finally {
            this.lock.unlock();
        }
        final InstallSnapshotDone installSnapshotDone = new InstallSnapshotDone(reader);
        if (!this.fsmCaller.onSnapshotLoad(installSnapshotDone)) {
            LOG.warn("Fail to call fsm onSnapshotLoad.");
            installSnapshotDone.run(new Status(RaftError.EHOSTDOWN, "This raft node is down"));
        }
    }

    @SuppressWarnings("all")
    boolean registerDownloadingSnapshot(final DownloadingSnapshot ds) {
        DownloadingSnapshot saved = null;
        boolean result = true;

        this.lock.lock();
        try {
            if (this.stopped) {
                LOG.warn("Register DownloadingSnapshot failed: node is stopped.");
                ds.done
                    .sendResponse(RaftRpcFactory.DEFAULT //
                        .newResponse(InstallSnapshotResponse.getDefaultInstance(), RaftError.EHOSTDOWN,
                            "Node is stopped."));
                return false;
            }
            if (this.savingSnapshot) {
                LOG.warn("Register DownloadingSnapshot failed: is saving snapshot.");
                ds.done.sendResponse(RaftRpcFactory.DEFAULT //
                    .newResponse(InstallSnapshotResponse.getDefaultInstance(), RaftError.EBUSY,
                        "Node is saving snapshot."));
                return false;
            }

            ds.responseBuilder.setTerm(this.term);
            if (ds.request.getTerm() != this.term) {
                LOG.warn("Register DownloadingSnapshot failed: term mismatch, expect {} but {}.", this.term,
                    ds.request.getTerm());
                ds.responseBuilder.setSuccess(false);
                ds.done.sendResponse(ds.responseBuilder.build());
                return false;
            }
            if (ds.request.getMeta().getLastIncludedIndex() <= this.lastSnapshotIndex) {
                LOG.warn(
                    "Register DownloadingSnapshot failed: snapshot is not newer, request lastIncludedIndex={}, lastSnapshotIndex={}.",
                    ds.request.getMeta().getLastIncludedIndex(), this.lastSnapshotIndex);
                ds.responseBuilder.setSuccess(true);
                ds.done.sendResponse(ds.responseBuilder.build());
                return false;
            }
            final DownloadingSnapshot m = this.downloadingSnapshot.get();
            if (m == null) {
                this.downloadingSnapshot.set(ds);
                Requires.requireTrue(this.curCopier == null, "Current copier is not null");
                this.curCopier = this.snapshotStorage.startToCopyFrom(ds.request.getUri(), newCopierOpts());
                if (this.curCopier == null) {
                    this.downloadingSnapshot.set(null);
                    LOG.warn("Register DownloadingSnapshot failed: fail to copy file from {}.", ds.request.getUri());
                    ds.done.sendResponse(RaftRpcFactory.DEFAULT //
                        .newResponse(InstallSnapshotResponse.getDefaultInstance(), RaftError.EINVAL,
                            "Fail to copy from: %s", ds.request.getUri()));
                    return false;
                }
                this.runningJobs.incrementAndGet();
                return true;
            }

            // A previous snapshot is under installing, check if this is the same
            // snapshot and resume it, otherwise drop previous snapshot as this one is
            // newer

            if (m.request.getMeta().getLastIncludedIndex() == ds.request.getMeta().getLastIncludedIndex()) {
                // m is a retry
                // Copy |*ds| to |*m| so that the former session would respond
                // this RPC.
                saved = m;
                this.downloadingSnapshot.set(ds);
                result = false;
            }
            else if (m.request.getMeta().getLastIncludedIndex() > ds.request.getMeta().getLastIncludedIndex()) {
                // |is| is older
                LOG.warn("Register DownloadingSnapshot failed: is installing a newer one, lastIncludeIndex={}.",
                    m.request.getMeta().getLastIncludedIndex());
                ds.done.sendResponse(RaftRpcFactory.DEFAULT //
                    .newResponse(InstallSnapshotResponse.getDefaultInstance(), RaftError.EINVAL,
                        "A newer snapshot is under installing"));
                return false;
            }
            else {
                // |is| is newer
                if (this.loadingSnapshot) {
                    LOG.warn("Register DownloadingSnapshot failed: is loading an older snapshot, lastIncludeIndex={}.",
                        m.request.getMeta().getLastIncludedIndex());
                    ds.done.sendResponse(RaftRpcFactory.DEFAULT //
                        .newResponse(InstallSnapshotResponse.getDefaultInstance(), RaftError.EBUSY,
                            "A former snapshot is under loading"));
                    return false;
                }
                Requires.requireNonNull(this.curCopier, "curCopier");
                this.curCopier.cancel();
                LOG.warn(
                    "Register DownloadingSnapshot failed: an older snapshot is under installing, cancel downloading, lastIncludeIndex={}.",
                    m.request.getMeta().getLastIncludedIndex());
                ds.done.sendResponse(RaftRpcFactory.DEFAULT //
                    .newResponse(InstallSnapshotResponse.getDefaultInstance(), RaftError.EBUSY,
                        "A former snapshot is under installing, trying to cancel"));
                return false;
            }
        }
        finally {
            this.lock.unlock();
        }
        if (saved != null) {
            // Respond replaced session
            LOG.warn("Register DownloadingSnapshot failed: interrupted by retry installling request.");
            saved.done.sendResponse(RaftRpcFactory.DEFAULT //
                .newResponse(InstallSnapshotResponse.getDefaultInstance(), RaftError.EINTR,
                    "Interrupted by the retry InstallSnapshotRequest"));
        }
        return result;
    }

    private SnapshotCopierOptions newCopierOpts() {
        final SnapshotCopierOptions copierOpts = new SnapshotCopierOptions();
        copierOpts.setNodeOptions(this.node.getOptions());
        copierOpts.setRaftClientService(this.node.getRpcClientService());
        copierOpts.setTimerManager(this.node.getTimerManager());
        copierOpts.setRaftOptions(this.node.getRaftOptions());
        return copierOpts;
    }

    @Override
    public void interruptDownloadingSnapshots(final long newTerm) {
        this.lock.lock();
        try {
            Requires.requireTrue(newTerm >= this.term);
            this.term = newTerm;
            if (this.downloadingSnapshot.get() == null) {
                return;
            }
            if (this.loadingSnapshot) {
                // We can't interrupt loading
                return;
            }
            Requires.requireNonNull(this.curCopier, "curCopier");
            this.curCopier.cancel();
            LOG.info("Trying to cancel downloading snapshot: {}.", this.downloadingSnapshot.get().request);
        }
        finally {
            this.lock.unlock();
        }
    }

    private void reportError(final int errCode, final String fmt, final Object... args) {
        final RaftException error = new RaftException(ErrorType.ERROR_TYPE_SNAPSHOT);
        error.setStatus(new Status(errCode, fmt, args));
        this.fsmCaller.onError(error);
    }

    @Override
    public boolean isInstallingSnapshot() {
        return this.downloadingSnapshot.get() != null;
    }

    @Override
    public SnapshotStorage getSnapshotStorage() {
        return this.snapshotStorage;
    }

    @Override
    public void join() throws InterruptedException {
        this.runningJobs.await();
    }

    @Override
    public void describe(final Printer out) {
        final long _lastSnapshotTerm;
        final long _lastSnapshotIndex;
        final long _term;
        final boolean _savingSnapshot;
        final boolean _loadingSnapshot;
        final boolean _stopped;
        this.lock.lock();
        try {
            _lastSnapshotTerm = this.lastSnapshotTerm;
            _lastSnapshotIndex = this.lastSnapshotIndex;
            _term = this.term;
            _savingSnapshot = this.savingSnapshot;
            _loadingSnapshot = this.loadingSnapshot;
            _stopped = this.stopped;
        }
        finally {
            this.lock.unlock();
        }
        out.print("  lastSnapshotTerm: ") //
            .println(_lastSnapshotTerm);
        out.print("  lastSnapshotIndex: ") //
            .println(_lastSnapshotIndex);
        out.print("  term: ") //
            .println(_term);
        out.print("  savingSnapshot: ") //
            .println(_savingSnapshot);
        out.print("  loadingSnapshot: ") //
            .println(_loadingSnapshot);
        out.print("  stopped: ") //
            .println(_stopped);
    }
}
