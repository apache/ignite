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

package org.apache.ignite.raft.client.service;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;
import org.apache.ignite.raft.client.Command;
import org.apache.ignite.raft.client.Peer;
import org.apache.ignite.raft.client.ReadCommand;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * A service providing operations on a replication group.
 * <p>
 * Most of operations require a known group leader. The group leader can be refreshed at any time by calling
 * {@link #refreshLeader()} method, otherwise it will happen automatically on a first call.
 * <p>
 * If a leader has been changed while the operation in progress, the operation will be transparently retried until
 * timeout is reached. The current leader will be refreshed automatically (maybe several times) in the process.
 * <p>
 * Each asynchronous method (returning a future) uses a default timeout to finish, see {@link #timeout()}.
 * If a result is not available within the timeout, the future will be completed with a {@link TimeoutException}
 * <p>
 * If an error is occured during operation execution, the future will be completed with the corresponding
 * IgniteException having an error code and a related message.
 * <p>
 * All async operations provided by the service are not cancellable.
 */
public interface RaftGroupService {
    /**
     * @return Group id.
     */
    @NotNull String groupId();

    /**
     * @return Default timeout for the operations in milliseconds.
     */
    long timeout();

    /**
     * Changes default timeout value for all subsequent operations.
     *
     * @param newTimeout New timeout value.
     */
    void timeout(long newTimeout);

    /**
     * @return Current leader id or {@code null} if it has not been yet initialized.
     */
    @Nullable Peer leader();

    /**
     * @return A list of voting peers or {@code null} if it has not been yet initialized. The order is corresponding
     * to the time of joining to the replication group.
     */
    @Nullable List<Peer> peers();

    /**
     * @return A list of leaners or {@code null} if it has not been yet initialized. The order is corresponding
     * to the time of joining to the replication group.
     */
    @Nullable List<Peer> learners();

    /**
     * Refreshes a replication group leader.
     * <p>
     * After the future completion the method {@link #leader()}
     * can be used to retrieve a current group leader.
     * <p>
     * This operation is executed on a group leader.
     *
     * @return A future.
     */
    CompletableFuture<Void> refreshLeader();

    /**
     * Refreshes replication group members.
     * <p>
     * After the future completion methods like {@link #peers()} and {@link #learners()}
     * can be used to retrieve current members of a group.
     * <p>
     * This operation is executed on a group leader.
     *
     * @param onlyAlive {@code True} to exclude dead nodes.
     * @return A future.
     */
    CompletableFuture<Void> refreshMembers(boolean onlyAlive);

    /**
     * Adds a voting peer to the replication group.
     * <p>
     * After the future completion methods like {@link #peers()} and {@link #learners()}
     * can be used to retrieve current members of a group.
     * <p>
     * This operation is executed on a group leader.
     *
     * @param peer Peer
     * @return A future.
     */
    CompletableFuture<Void> addPeer(Peer peer);

    /**
     * Removes peer from the replication group.
     * <p>
     * After the future completion methods like {@link #peers()} and {@link #learners()}
     * can be used to retrieve current members of a group.
     * <p>
     * This operation is executed on a group leader.
     *
     * @param peer Peer.
     * @return A future.
     */
    CompletableFuture<Void> removePeer(Peer peer);

    /**
     * Changes peers of the replication group.
     * <p>
     * After the future completion methods like {@link #peers()} and {@link #learners()}
     * can be used to retrieve current members of a group.
     * <p>
     * This operation is executed on a group leader.
     *
     * @param peers Peers.
     * @return A future.
     */
    CompletableFuture<Void> changePeers(List<Peer> peers);

    /**
     * Adds learners (non-voting members).
     * <p>
     * After the future completion methods like {@link #peers()} and {@link #learners()}
     * can be used to retrieve current members of a group.
     * <p>
     * This operation is executed on a group leader.
     *
     * @param learners List of learners.
     * @return A future.
     */
    CompletableFuture<Void> addLearners(List<Peer> learners);

    /**
     * Removes learners.
     * <p>
     * After the future completion methods like {@link #peers()} and {@link #learners()}
     * can be used to retrieve current members of a group.
     * <p>
     * This operation is executed on a group leader.
     *
     * @param learners List of learners.
     * @return A future.
     */
    CompletableFuture<Void> removeLearners(List<Peer> learners);

    /**
     * Set learners of the raft group to needed list of learners.
     * <p>
     * After the future completion methods like {@link #peers()} and {@link #learners()}
     * can be used to retrieve current members of a group.
     * <p>
     * This operation is executed on a group leader.
     *
     * @param learners List of learners.
     * @return A future.
     */
    CompletableFuture<Void> resetLearners(List<Peer> learners);

    /**
     * Takes a state machine snapshot on a given group peer.
     *
     * @param peer Peer.
     * @return A future.
     */
    CompletableFuture<Void> snapshot(Peer peer);

    /**
     * Transfers leadership to other peer.
     * <p>
     * This operation is executed on a group leader.
     *
     * @param newLeader New leader.
     * @return A future.
     */
    CompletableFuture<Void> transferLeadership(Peer newLeader);

    /**
     * Runs a command on a replication group leader.
     * <p>
     * Read commands always see up to date data.
     *
     * @param cmd The command.
     * @param <R> Execution result type.
     * @return A future with the execution result.
     */
    <R> CompletableFuture<R> run(Command cmd);

    /**
     * Runs a read command on a given peer.
     * <p>
     * Read commands can see stale data (in the past).
     *
     * @param peer Peer id.
     * @param cmd The command.
     * @param <R> Execution result type.
     * @return A future with the execution result.
     */
    <R> CompletableFuture<R> run(Peer peer, ReadCommand cmd);

    /**
     * Shutdown and cleanup resources for this instance.
     */
    void shutdown();
}
