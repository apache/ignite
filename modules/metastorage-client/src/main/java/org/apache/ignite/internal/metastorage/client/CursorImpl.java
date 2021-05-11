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

package org.apache.ignite.internal.metastorage.client;

import java.util.Iterator;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import org.apache.ignite.internal.metastorage.common.command.cursor.CursorCloseCommand;
import org.apache.ignite.internal.metastorage.common.command.cursor.CursorHasNextCommand;
import org.apache.ignite.internal.metastorage.common.command.cursor.CursorNextCommand;
import org.apache.ignite.lang.IgniteInternalException;
import org.apache.ignite.lang.IgniteLogger;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.metastorage.common.Cursor;
import org.apache.ignite.raft.client.service.RaftGroupService;
import org.jetbrains.annotations.NotNull;

/**
 * Meta storage service side implementation of cursor.
 * @param <T> Cursor parameter.
 */
public class CursorImpl<T> implements Cursor<T> {
    /** The logger. */
    private static final IgniteLogger LOG = IgniteLogger.forClass(CursorImpl.class);

    /** Future that runs meta storage service operation that provides cursor. */
    private CompletableFuture<IgniteUuid> initOp;

    /** Meta storage raft group service. */
    private RaftGroupService metaStorageRaftGrpSvc;

    /**
     * @param metaStorageRaftGrpSvc Meta storage raft group service.
     * @param initOp Future that runs meta storage service operation that provides cursor.
     */
    CursorImpl(RaftGroupService metaStorageRaftGrpSvc, CompletableFuture<IgniteUuid> initOp) {
        this.metaStorageRaftGrpSvc = metaStorageRaftGrpSvc;
        this.initOp = initOp;
    }

    /** {@inheritDoc} */
    @NotNull @Override public Iterator<T> iterator() {
        return new Iterator<>() {
            /** {@inheritDoc} */
            @Override public boolean hasNext() {
                try {
                    return initOp.thenCompose(
                        cursorId -> metaStorageRaftGrpSvc.<Boolean>run(new CursorHasNextCommand(cursorId))).get();
                }
                catch (InterruptedException | ExecutionException e) {
                    LOG.error("Unable to evaluate cursor hasNext command", e);

                    throw new IgniteInternalException(e);
                }
            }

            /** {@inheritDoc} */
            @Override public T next() {
                try {
                    return initOp.thenCompose(
                        cursorId -> metaStorageRaftGrpSvc.<T>run(new CursorNextCommand(cursorId))).get();
                }
                catch (InterruptedException | ExecutionException e) {
                    LOG.error("Unable to evaluate cursor hasNext command", e);

                    throw new IgniteInternalException(e);
                }
            }
        };
    }

    /** {@inheritDoc} */
    @Override public void close() {
        try {
            initOp.thenCompose(
                cursorId -> metaStorageRaftGrpSvc.run(new CursorCloseCommand(cursorId))).get();
        }
        catch (InterruptedException | ExecutionException e) {
            LOG.error("Unable to evaluate cursor close command", e);

            throw new IgniteInternalException(e);
        }
    }
}
