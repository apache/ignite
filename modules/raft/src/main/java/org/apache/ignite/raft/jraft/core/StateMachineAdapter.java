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

import org.apache.ignite.raft.jraft.Closure;
import org.apache.ignite.raft.jraft.Iterator;
import org.apache.ignite.raft.jraft.StateMachine;
import org.apache.ignite.raft.jraft.Status;
import org.apache.ignite.raft.jraft.conf.Configuration;
import org.apache.ignite.raft.jraft.entity.LeaderChangeContext;
import org.apache.ignite.raft.jraft.error.RaftException;
import org.apache.ignite.raft.jraft.storage.snapshot.SnapshotReader;
import org.apache.ignite.raft.jraft.storage.snapshot.SnapshotWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * State machine adapter that implements all methods with default behavior except {@link #onApply(Iterator)}.
 */
public abstract class StateMachineAdapter implements StateMachine {
    /** The logger */
    private static final Logger LOG = LoggerFactory.getLogger(StateMachineAdapter.class);

    @Override
    public void onShutdown() {
        LOG.info("onShutdown.");
    }

    @Override
    public void onSnapshotSave(final SnapshotWriter writer, final Closure done) {
        error("onSnapshotSave");
        runClosure(done, "onSnapshotSave");
    }

    @Override
    public boolean onSnapshotLoad(final SnapshotReader reader) {
        error("onSnapshotLoad", "while a snapshot is saved in " + reader.getPath());
        return false;
    }

    @Override
    public void onLeaderStart(final long term) {
        LOG.info("onLeaderStart: term={}.", term);
    }

    @Override
    public void onLeaderStop(final Status status) {
        LOG.info("onLeaderStop: status={}.", status);
    }

    @Override
    public void onError(final RaftException e) {
        LOG.error(
            "Encountered an error={} on StateMachine {}, it's highly recommended to implement this method as raft stops working since some error occurs, you should figure out the cause and repair or remove this node.",
            e.getStatus(), getClassName(), e);
    }

    @Override
    public void onConfigurationCommitted(final Configuration conf) {
        LOG.info("onConfigurationCommitted: {}.", conf);
    }

    @Override
    public void onStopFollowing(final LeaderChangeContext ctx) {
        LOG.info("onStopFollowing: {}.", ctx);
    }

    @Override
    public void onStartFollowing(final LeaderChangeContext ctx) {
        LOG.info("onStartFollowing: {}.", ctx);
    }

    @SuppressWarnings("SameParameterValue")
    private void runClosure(final Closure done, final String methodName) {
        done.run(new Status(-1, "%s doesn't implement %s", getClassName(), methodName));
    }

    private String getClassName() {
        return getClass().getName();
    }

    @SuppressWarnings("SameParameterValue")
    private void error(final String methodName) {
        error(methodName, "");
    }

    private void error(final String methodName, final String msg) {
        LOG.error("{} doesn't implement {} {}.", getClassName(), methodName, msg);
    }
}
