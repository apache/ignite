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

package org.apache.ignite.raft.server;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import org.apache.ignite.lang.IgniteLogger;
import org.apache.ignite.raft.client.ReadCommand;
import org.apache.ignite.raft.client.WriteCommand;
import org.apache.ignite.raft.client.service.CommandClosure;
import org.apache.ignite.raft.client.service.RaftGroupListener;
import org.apache.ignite.raft.jraft.util.ExecutorServiceHelper;
import org.apache.ignite.raft.jraft.util.Utils;

/**
 * The counter listener implementation.
 */
public class CounterListener implements RaftGroupListener {
    /**
     * The logger.
     */
    private static final IgniteLogger LOG = IgniteLogger.forClass(CounterListener.class);

    /**
     * The counter.
     */
    private AtomicLong counter = new AtomicLong();

    /**
     * Snapshot executor.
     */
    private ExecutorService executor = Executors.newSingleThreadExecutor();

    /** {@inheritDoc} */
    @Override public void onRead(Iterator<CommandClosure<ReadCommand>> iterator) {
        while (iterator.hasNext()) {
            CommandClosure<ReadCommand> clo = iterator.next();

            assert clo.command() instanceof GetValueCommand;

            clo.result(counter.get());
        }
    }

    /** {@inheritDoc} */
    @Override public void onWrite(Iterator<CommandClosure<WriteCommand>> iterator) {
        while (iterator.hasNext()) {
            CommandClosure<WriteCommand> clo = iterator.next();

            IncrementAndGetCommand cmd0 = (IncrementAndGetCommand) clo.command();

            clo.result(counter.addAndGet(cmd0.delta()));
        }
    }

    /** {@inheritDoc} */
    @Override public void onSnapshotSave(Path path, Consumer<Throwable> doneClo) {
        final long currVal = this.counter.get();

        Utils.runInThread(executor, () -> {
            final CounterSnapshotFile snapshot = new CounterSnapshotFile(path + File.separator + "data");

            try {
                snapshot.save(currVal);

                doneClo.accept(null);
            }
            catch (Throwable e) {
                doneClo.accept(e);
            }
        });
    }

    /** {@inheritDoc} */
    @Override public boolean onSnapshotLoad(Path path) {
        final CounterSnapshotFile snapshot = new CounterSnapshotFile(path + File.separator + "data");
        try {
            this.counter.set(snapshot.load());
            return true;
        }
        catch (final IOException e) {
            LOG.error("Fail to load snapshot from {}", snapshot.getPath());
            return false;
        }
    }

    /** {@inheritDoc} */
    @Override public void onShutdown() {
        ExecutorServiceHelper.shutdownAndAwaitTermination(executor);
    }

    /**
     * @return Current value.
     */
    public long value() {
        return counter.get();
    }
}
