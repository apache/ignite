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
package org.apache.ignite.raft.jraft.storage.impl;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import org.apache.ignite.raft.jraft.core.NodeImpl;
import org.apache.ignite.raft.jraft.core.NodeMetrics;
import org.apache.ignite.raft.jraft.entity.EnumOutter.ErrorType;
import org.apache.ignite.raft.jraft.entity.LocalStorageOutter.StablePBMeta;
import org.apache.ignite.raft.jraft.entity.PeerId;
import org.apache.ignite.raft.jraft.error.RaftError;
import org.apache.ignite.raft.jraft.error.RaftException;
import org.apache.ignite.raft.jraft.option.RaftMetaStorageOptions;
import org.apache.ignite.raft.jraft.option.RaftOptions;
import org.apache.ignite.raft.jraft.storage.RaftMetaStorage;
import org.apache.ignite.raft.jraft.storage.io.MessageFile;
import org.apache.ignite.raft.jraft.util.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Raft meta storage,it's not thread-safe.
 */
public class LocalRaftMetaStorage implements RaftMetaStorage {

    private static final Logger LOG = LoggerFactory.getLogger(LocalRaftMetaStorage.class);
    private static final String RAFT_META = "raft_meta";

    private boolean isInited;
    private final String path;
    private long term;
    /** blank votedFor information */
    private PeerId votedFor = PeerId.emptyPeer();
    private final RaftOptions raftOptions;
    private NodeMetrics nodeMetrics;
    private NodeImpl node;

    public LocalRaftMetaStorage(final String path, final RaftOptions raftOptions) {
        super();
        this.path = path;
        this.raftOptions = raftOptions;
    }

    @Override
    public boolean init(final RaftMetaStorageOptions opts) {
        if (this.isInited) {
            LOG.warn("Raft meta storage is already inited.");
            return true;
        }
        this.node = opts.getNode();
        this.nodeMetrics = this.node.getNodeMetrics();
        File dir = new File(this.path);

        if (!Utils.mkdir(dir)) {
            LOG.error("Fail to mkdir {}", this.path);
            return false;
        }

        if (load()) {
            this.isInited = true;
            return true;
        }
        else {
            return false;
        }
    }

    private boolean load() {
        final MessageFile pbFile = newPbFile();
        try {
            final StablePBMeta meta = pbFile.load();
            if (meta != null) {
                this.term = meta.getTerm();
                return this.votedFor.parse(meta.getVotedfor());
            }
            return true;
        }
        catch (final FileNotFoundException e) {
            return true;
        }
        catch (final IOException e) {
            LOG.error("Fail to load raft meta storage", e);
            return false;
        }
    }

    private MessageFile newPbFile() {
        return new MessageFile(this.path + File.separator + RAFT_META);
    }

    private boolean save() {
        final long start = Utils.monotonicMs();
        final StablePBMeta meta = StablePBMeta.newBuilder() //
            .setTerm(this.term) //
            .setVotedfor(this.votedFor.toString()) //
            .build();
        final MessageFile pbFile = newPbFile();
        try {
            if (!pbFile.save(meta, this.raftOptions.isSyncMeta())) {
                reportIOError();
                return false;
            }
            return true;
        }
        catch (final Exception e) {
            LOG.error("Fail to save raft meta", e);
            reportIOError();
            return false;
        }
        finally {
            final long cost = Utils.monotonicMs() - start;
            if (this.nodeMetrics != null) {
                this.nodeMetrics.recordLatency("save-raft-meta", cost);
            }
            LOG.info("Save raft meta, path={}, term={}, votedFor={}, cost time={} ms", this.path, this.term,
                this.votedFor, cost);
        }
    }

    private void reportIOError() {
        this.node.onError(new RaftException(ErrorType.ERROR_TYPE_META, RaftError.EIO,
            "Fail to save raft meta, path=%s", this.path));
    }

    @Override
    public void shutdown() {
        if (!this.isInited) {
            return;
        }
        save();
        this.isInited = false;
    }

    private void checkState() {
        if (!this.isInited) {
            throw new IllegalStateException("LocalRaftMetaStorage not initialized");
        }
    }

    @Override
    public boolean setTerm(final long term) {
        checkState();
        this.term = term;
        return save();
    }

    @Override
    public long getTerm() {
        checkState();
        return this.term;
    }

    @Override
    public boolean setVotedFor(final PeerId peerId) {
        checkState();
        this.votedFor = peerId;
        return save();
    }

    @Override
    public PeerId getVotedFor() {
        checkState();
        return this.votedFor;
    }

    @Override
    public boolean setTermAndVotedFor(final long term, final PeerId peerId) {
        checkState();
        this.votedFor = peerId;
        this.term = term;
        return save();
    }

    @Override
    public String toString() {
        return "RaftMetaStorageImpl [path=" + this.path + ", term=" + this.term + ", votedFor=" + this.votedFor + "]";
    }
}
