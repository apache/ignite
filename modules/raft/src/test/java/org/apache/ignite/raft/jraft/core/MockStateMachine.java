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
package org.apache.ignite.raft.jraft.core;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.ignite.lang.IgniteLogger;
import org.apache.ignite.raft.jraft.Closure;
import org.apache.ignite.raft.jraft.Iterator;
import org.apache.ignite.raft.jraft.Status;
import org.apache.ignite.raft.jraft.entity.LeaderChangeContext;
import org.apache.ignite.raft.jraft.error.RaftError;
import org.apache.ignite.raft.jraft.storage.snapshot.SnapshotReader;
import org.apache.ignite.raft.jraft.storage.snapshot.SnapshotWriter;
import org.apache.ignite.raft.jraft.util.Bits;
import org.apache.ignite.raft.jraft.util.Endpoint;

public class MockStateMachine extends StateMachineAdapter {
    private static final IgniteLogger LOG = IgniteLogger.forClass(MockStateMachine.class);

    private final Lock lock = new ReentrantLock();
    private volatile int onStartFollowingTimes = 0;
    private volatile int onStopFollowingTimes = 0;
    private volatile long leaderTerm = -1;
    private volatile long appliedIndex = -1;
    private volatile long snapshotIndex = -1L;
    private final List<ByteBuffer> logs = new ArrayList<>();
    private final Endpoint address;
    private volatile int saveSnapshotTimes;
    private volatile int loadSnapshotTimes;

    public Endpoint getAddress() {
        return this.address;
    }

    public MockStateMachine(final Endpoint address) {
        super();
        this.address = address;
    }

    public int getSaveSnapshotTimes() {
        return this.saveSnapshotTimes;
    }

    public int getLoadSnapshotTimes() {
        return this.loadSnapshotTimes;
    }

    public int getOnStartFollowingTimes() {
        return this.onStartFollowingTimes;
    }

    public int getOnStopFollowingTimes() {
        return this.onStopFollowingTimes;
    }

    public long getLeaderTerm() {
        return this.leaderTerm;
    }

    public long getAppliedIndex() {
        return this.appliedIndex;
    }

    public long getSnapshotIndex() {
        return this.snapshotIndex;
    }

    public void lock() {
        this.lock.lock();
    }

    public void unlock() {
        this.lock.unlock();
    }

    public List<ByteBuffer> getLogs() {
        this.lock.lock();
        try {
            return this.logs;
        }
        finally {
            this.lock.unlock();
        }
    }

    private final AtomicLong lastAppliedIndex = new AtomicLong(-1);

    @Override
    public void onApply(final Iterator iter) {
        while (iter.hasNext()) {
            this.lock.lock();
            try {
                if (iter.getIndex() <= this.lastAppliedIndex.get()) {
                    // prevent duplication
                    continue;
                }
                this.lastAppliedIndex.set(iter.getIndex());
                this.logs.add(iter.getData().slice());
                if (iter.done() != null) {
                    iter.done().run(Status.OK());
                }
            }
            finally {
                this.lock.unlock();
            }
            this.appliedIndex = iter.getIndex();
            iter.next();
        }
    }

    public boolean isLeader() {
        return this.leaderTerm > 0;
    }

    @Override
    public void onSnapshotSave(final SnapshotWriter writer, final Closure done) {
        this.saveSnapshotTimes++;
        final String path = writer.getPath() + File.separator + "data";
        final File file = new File(path);
        try (FileOutputStream fout = new FileOutputStream(file);
             BufferedOutputStream out = new BufferedOutputStream(fout)) {
            this.lock.lock();
            try {
                for (final ByteBuffer buf : this.logs) {
                    final byte[] bs = new byte[4];
                    Bits.putInt(bs, 0, buf.remaining());
                    out.write(bs);
                    out.write(buf.array());
                }
                this.snapshotIndex = this.appliedIndex;
            }
            finally {
                this.lock.unlock();
            }
            LOG.info("Node<" + this.address + "> saved snapshot into " + file);
            writer.addFile("data");
            done.run(Status.OK());
        }
        catch (final IOException e) {
            LOG.error("Failed to save the snapshot", e);
            done.run(new Status(RaftError.EIO, "Fail to save snapshot"));
        }
    }

    @Override
    public boolean onSnapshotLoad(final SnapshotReader reader) {
        this.lastAppliedIndex.set(0);
        this.loadSnapshotTimes++;
        final String path = reader.getPath() + File.separator + "data";
        final File file = new File(path);
        if (!file.exists()) {
            return false;
        }
        try (FileInputStream fin = new FileInputStream(file); BufferedInputStream in = new BufferedInputStream(fin)) {
            this.lock.lock();
            this.logs.clear();
            try {
                while (true) {
                    final byte[] bs = new byte[4];
                    if (in.read(bs) == 4) {
                        final int len = Bits.getInt(bs, 0);
                        final byte[] buf = new byte[len];
                        if (in.read(buf) != len) {
                            break;
                        }
                        this.logs.add(ByteBuffer.wrap(buf));
                    }
                    else {
                        break;
                    }
                }
            }
            finally {
                this.lock.unlock();
            }
            LOG.info("Node<" + this.address + "> loaded snapshot from " + path);
            return true;
        }
        catch (final IOException e) {
            LOG.error("Failed to load the snapshot", e);
            return false;
        }
    }

    @Override
    public void onLeaderStart(final long term) {
        super.onLeaderStart(term);
        this.leaderTerm = term;
    }

    @Override
    public void onLeaderStop(final Status status) {
        super.onLeaderStop(status);
        this.leaderTerm = -1;
    }

    @Override
    public void onStopFollowing(final LeaderChangeContext ctx) {
        super.onStopFollowing(ctx);
        this.onStopFollowingTimes++;
    }

    @Override
    public void onStartFollowing(final LeaderChangeContext ctx) {
        super.onStartFollowing(ctx);
        this.onStartFollowingTimes++;
    }

}
