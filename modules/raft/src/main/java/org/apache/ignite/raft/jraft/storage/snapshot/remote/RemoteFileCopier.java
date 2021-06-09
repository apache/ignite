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
package org.apache.ignite.raft.jraft.storage.snapshot.remote;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import org.apache.ignite.raft.jraft.core.Scheduler;
import org.apache.ignite.raft.jraft.option.CopyOptions;
import org.apache.ignite.raft.jraft.option.NodeOptions;
import org.apache.ignite.raft.jraft.option.RaftOptions;
import org.apache.ignite.raft.jraft.option.SnapshotCopierOptions;
import org.apache.ignite.raft.jraft.rpc.RaftClientService;
import org.apache.ignite.raft.jraft.rpc.RpcRequests.GetFileRequest;
import org.apache.ignite.raft.jraft.storage.SnapshotThrottle;
import org.apache.ignite.raft.jraft.storage.snapshot.Snapshot;
import org.apache.ignite.raft.jraft.util.ByteBufferCollector;
import org.apache.ignite.raft.jraft.util.Endpoint;
import org.apache.ignite.raft.jraft.util.OnlyForTest;
import org.apache.ignite.raft.jraft.util.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Remote file copier
 */
public class RemoteFileCopier {

    private static final Logger LOG = LoggerFactory.getLogger(RemoteFileCopier.class);

    private long readId;
    private RaftClientService rpcService;
    private Endpoint endpoint;
    private RaftOptions raftOptions;
    private NodeOptions nodeOptions;
    private Scheduler timerManager;
    private SnapshotThrottle snapshotThrottle;

    @OnlyForTest
    long getReaderId() {
        return this.readId;
    }

    @OnlyForTest
    Endpoint getEndpoint() {
        return this.endpoint;
    }

    public boolean init(String uri, final SnapshotThrottle snapshotThrottle, final SnapshotCopierOptions opts) {
        this.rpcService = opts.getRaftClientService();
        this.timerManager = opts.getTimerManager();
        this.raftOptions = opts.getRaftOptions();
        this.nodeOptions = opts.getNodeOptions();
        this.snapshotThrottle = snapshotThrottle;

        final int prefixSize = Snapshot.REMOTE_SNAPSHOT_URI_SCHEME.length();
        if (uri == null || !uri.startsWith(Snapshot.REMOTE_SNAPSHOT_URI_SCHEME)) {
            LOG.error("Invalid uri {}.", uri);
            return false;
        }
        uri = uri.substring(prefixSize);
        final int slasPos = uri.indexOf('/');
        final String ipAndPort = uri.substring(0, slasPos);
        uri = uri.substring(slasPos + 1);

        try {
            this.readId = Long.parseLong(uri);
            final String[] ipAndPortStrs = ipAndPort.split(":");
            this.endpoint = new Endpoint(ipAndPortStrs[0], Integer.parseInt(ipAndPortStrs[1]));
        }
        catch (final Exception e) {
            LOG.error("Fail to parse readerId or endpoint.", e);
            return false;
        }
        if (!this.rpcService.connect(this.endpoint)) {
            LOG.error("Fail to init channel to {}.", this.endpoint);
            return false;
        }

        return true;
    }

    /**
     * Copy `source` from remote to local dest.
     *
     * @param source source from remote
     * @param destPath local path
     * @param opts options of copy
     * @return true if copy success
     */
    public boolean copyToFile(final String source, final String destPath, final CopyOptions opts) throws IOException,
        InterruptedException {
        final Session session = startCopyToFile(source, destPath, opts);
        if (session == null) {
            return false;
        }
        try {
            session.join();
            return session.status().isOk();
        }
        finally {
            Utils.closeQuietly(session);
        }
    }

    public Session startCopyToFile(final String source, final String destPath, final CopyOptions opts)
        throws IOException {
        final File file = new File(destPath);

        // delete exists file.
        if (file.exists()) {
            if (!file.delete()) {
                LOG.error("Fail to delete destPath: {}.", destPath);
                return null;
            }
        }

        final OutputStream out = new BufferedOutputStream(new FileOutputStream(file, false) {

            @Override
            public void close() throws IOException {
                getFD().sync();
                super.close();
            }
        });
        final CopySession session = newCopySession(source);
        session.setOutputStream(out);
        session.setDestPath(destPath);
        session.setDestBuf(null);
        if (opts != null) {
            session.setCopyOptions(opts);
        }
        session.sendNextRpc();
        return session;
    }

    private CopySession newCopySession(final String source) {
        final GetFileRequest.Builder reqBuilder = GetFileRequest.newBuilder() //
            .setFilename(source) //
            .setReaderId(this.readId);
        return new CopySession(this.rpcService, this.timerManager, this.snapshotThrottle, this.raftOptions, this.nodeOptions, reqBuilder,
            this.endpoint);
    }

    /**
     * Copy `source` from remote to  buffer.
     *
     * @param source source from remote
     * @param destBuf buffer of dest
     * @param opt options of copy
     * @return true if copy success
     */
    public boolean copy2IoBuffer(final String source, final ByteBufferCollector destBuf, final CopyOptions opt)
        throws InterruptedException {
        final Session session = startCopy2IoBuffer(source, destBuf, opt);
        if (session == null) {
            return false;
        }
        try {
            session.join();
            return session.status().isOk();
        }
        finally {
            Utils.closeQuietly(session);
        }
    }

    public Session startCopy2IoBuffer(final String source, final ByteBufferCollector destBuf, final CopyOptions opts) {
        final CopySession session = newCopySession(source);
        session.setOutputStream(null);
        session.setDestBuf(destBuf);
        if (opts != null) {
            session.setCopyOptions(opts);
        }
        session.sendNextRpc();
        return session;
    }
}
