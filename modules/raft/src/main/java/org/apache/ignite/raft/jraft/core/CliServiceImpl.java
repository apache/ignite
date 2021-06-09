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

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.raft.jraft.CliService;
import org.apache.ignite.raft.jraft.Status;
import org.apache.ignite.raft.jraft.conf.Configuration;
import org.apache.ignite.raft.jraft.entity.PeerId;
import org.apache.ignite.raft.jraft.error.JRaftException;
import org.apache.ignite.raft.jraft.error.RaftError;
import org.apache.ignite.raft.jraft.option.CliOptions;
import org.apache.ignite.raft.jraft.rpc.CliClientService;
import org.apache.ignite.raft.jraft.rpc.CliRequests.AddLearnersRequest;
import org.apache.ignite.raft.jraft.rpc.CliRequests.AddPeerRequest;
import org.apache.ignite.raft.jraft.rpc.CliRequests.AddPeerResponse;
import org.apache.ignite.raft.jraft.rpc.CliRequests.ChangePeersRequest;
import org.apache.ignite.raft.jraft.rpc.CliRequests.ChangePeersResponse;
import org.apache.ignite.raft.jraft.rpc.CliRequests.GetLeaderRequest;
import org.apache.ignite.raft.jraft.rpc.CliRequests.GetLeaderResponse;
import org.apache.ignite.raft.jraft.rpc.CliRequests.GetPeersRequest;
import org.apache.ignite.raft.jraft.rpc.CliRequests.GetPeersResponse;
import org.apache.ignite.raft.jraft.rpc.CliRequests.LearnersOpResponse;
import org.apache.ignite.raft.jraft.rpc.CliRequests.RemoveLearnersRequest;
import org.apache.ignite.raft.jraft.rpc.CliRequests.RemovePeerRequest;
import org.apache.ignite.raft.jraft.rpc.CliRequests.RemovePeerResponse;
import org.apache.ignite.raft.jraft.rpc.CliRequests.ResetLearnersRequest;
import org.apache.ignite.raft.jraft.rpc.CliRequests.ResetPeerRequest;
import org.apache.ignite.raft.jraft.rpc.CliRequests.SnapshotRequest;
import org.apache.ignite.raft.jraft.rpc.CliRequests.TransferLeaderRequest;
import org.apache.ignite.raft.jraft.rpc.Message;
import org.apache.ignite.raft.jraft.rpc.RpcRequests.ErrorResponse;
import org.apache.ignite.raft.jraft.rpc.impl.cli.CliClientServiceImpl;
import org.apache.ignite.raft.jraft.util.Requires;
import org.apache.ignite.raft.jraft.util.StringUtils;
import org.apache.ignite.raft.jraft.util.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Cli service implementation.
 */
public class CliServiceImpl implements CliService {
    private static final Logger LOG = LoggerFactory.getLogger(CliServiceImpl.class);

    private CliOptions cliOptions;
    private CliClientService cliClientService;

    @Override
    public synchronized boolean init(final CliOptions opts) {
        Requires.requireNonNull(opts, "Null cli options");

        if (this.cliClientService != null) {
            return true;
        }
        this.cliOptions = opts;
        this.cliClientService = new CliClientServiceImpl();
        return this.cliClientService.init(this.cliOptions);
    }

    @Override
    public synchronized void shutdown() {
        if (this.cliClientService == null) {
            return;
        }
        this.cliClientService.shutdown();
        this.cliClientService = null;
    }

    @Override
    public Status addPeer(final String groupId, final Configuration conf, final PeerId peer) {
        Requires.requireTrue(!StringUtils.isBlank(groupId), "Blank group id");
        Requires.requireNonNull(conf, "Null configuration");
        Requires.requireNonNull(peer, "Null peer");

        final PeerId leaderId = new PeerId();
        final Status st = getLeader(groupId, conf, leaderId);
        if (!st.isOk()) {
            return st;
        }

        if (!this.cliClientService.connect(leaderId.getEndpoint())) {
            return new Status(-1, "Fail to init channel to leader %s", leaderId);
        }
        final AddPeerRequest.Builder rb = AddPeerRequest.newBuilder() //
            .setGroupId(groupId) //
            .setLeaderId(leaderId.toString()) //
            .setPeerId(peer.toString());

        try {
            final Message result = this.cliClientService.addPeer(leaderId.getEndpoint(), rb.build(), null).get();
            if (result instanceof AddPeerResponse) {
                final AddPeerResponse resp = (AddPeerResponse) result;
                final Configuration oldConf = new Configuration();
                for (final String peerIdStr : resp.getOldPeersList()) {
                    final PeerId oldPeer = new PeerId();
                    oldPeer.parse(peerIdStr);
                    oldConf.addPeer(oldPeer);
                }
                final Configuration newConf = new Configuration();
                for (final String peerIdStr : resp.getNewPeersList()) {
                    final PeerId newPeer = new PeerId();
                    newPeer.parse(peerIdStr);
                    newConf.addPeer(newPeer);
                }

                LOG.info("Configuration of replication group {} changed from {} to {}.", groupId, oldConf, newConf);
                return Status.OK();
            }
            else {
                return statusFromResponse(result);
            }

        }
        catch (final Exception e) {
            return new Status(-1, e.getMessage());
        }
    }

    private Status statusFromResponse(final Message result) {
        final ErrorResponse resp = (ErrorResponse) result;
        return new Status(resp.getErrorCode(), resp.getErrorMsg());
    }

    @Override
    public Status removePeer(final String groupId, final Configuration conf, final PeerId peer) {
        Requires.requireTrue(!StringUtils.isBlank(groupId), "Blank group id");
        Requires.requireNonNull(conf, "Null configuration");
        Requires.requireNonNull(peer, "Null peer");
        Requires.requireTrue(!peer.isEmpty(), "Removing peer is blank");

        final PeerId leaderId = new PeerId();
        final Status st = getLeader(groupId, conf, leaderId);
        if (!st.isOk()) {
            return st;
        }

        if (!this.cliClientService.connect(leaderId.getEndpoint())) {
            return new Status(-1, "Fail to init channel to leader %s", leaderId);
        }

        final RemovePeerRequest.Builder rb = RemovePeerRequest.newBuilder() //
            .setGroupId(groupId) //
            .setLeaderId(leaderId.toString()) //
            .setPeerId(peer.toString());

        try {
            final Message result = this.cliClientService.removePeer(leaderId.getEndpoint(), rb.build(), null).get();
            if (result instanceof RemovePeerResponse) {
                final RemovePeerResponse resp = (RemovePeerResponse) result;
                final Configuration oldConf = new Configuration();
                for (final String peerIdStr : resp.getOldPeersList()) {
                    final PeerId oldPeer = new PeerId();
                    oldPeer.parse(peerIdStr);
                    oldConf.addPeer(oldPeer);
                }
                final Configuration newConf = new Configuration();
                for (final String peerIdStr : resp.getNewPeersList()) {
                    final PeerId newPeer = new PeerId();
                    newPeer.parse(peerIdStr);
                    newConf.addPeer(newPeer);
                }

                LOG.info("Configuration of replication group {} changed from {} to {}", groupId, oldConf, newConf);
                return Status.OK();
            }
            else {
                return statusFromResponse(result);

            }
        }
        catch (final Exception e) {
            return new Status(-1, e.getMessage());
        }
    }

    // TODO refactor addPeer/removePeer/changePeers/transferLeader, remove duplicated code IGNITE-14832
    @Override
    public Status changePeers(final String groupId, final Configuration conf, final Configuration newPeers) {
        Requires.requireTrue(!StringUtils.isBlank(groupId), "Blank group id");
        Requires.requireNonNull(conf, "Null configuration");
        Requires.requireNonNull(newPeers, "Null new peers");

        final PeerId leaderId = new PeerId();
        final Status st = getLeader(groupId, conf, leaderId);
        if (!st.isOk()) {
            return st;
        }

        if (!this.cliClientService.connect(leaderId.getEndpoint())) {
            return new Status(-1, "Fail to init channel to leader %s", leaderId);
        }

        final ChangePeersRequest.Builder rb = ChangePeersRequest.newBuilder() //
            .setGroupId(groupId) //
            .setLeaderId(leaderId.toString());
        for (final PeerId peer : newPeers) {
            rb.addNewPeers(peer.toString());
        }

        try {
            final Message result = this.cliClientService.changePeers(leaderId.getEndpoint(), rb.build(), null).get();
            if (result instanceof ChangePeersResponse) {
                final ChangePeersResponse resp = (ChangePeersResponse) result;
                final Configuration oldConf = new Configuration();
                for (final String peerIdStr : resp.getOldPeersList()) {
                    final PeerId oldPeer = new PeerId();
                    oldPeer.parse(peerIdStr);
                    oldConf.addPeer(oldPeer);
                }
                final Configuration newConf = new Configuration();
                for (final String peerIdStr : resp.getNewPeersList()) {
                    final PeerId newPeer = new PeerId();
                    newPeer.parse(peerIdStr);
                    newConf.addPeer(newPeer);
                }

                LOG.info("Configuration of replication group {} changed from {} to {}", groupId, oldConf, newConf);
                return Status.OK();
            }
            else {
                return statusFromResponse(result);

            }
        }
        catch (final Exception e) {
            return new Status(-1, e.getMessage());
        }
    }

    @Override
    public Status resetPeer(final String groupId, final PeerId peerId, final Configuration newPeers) {
        Requires.requireTrue(!StringUtils.isBlank(groupId), "Blank group id");
        Requires.requireNonNull(peerId, "Null peerId");
        Requires.requireNonNull(newPeers, "Null new peers");

        if (!this.cliClientService.connect(peerId.getEndpoint())) {
            return new Status(-1, "Fail to init channel to %s", peerId);
        }

        final ResetPeerRequest.Builder rb = ResetPeerRequest.newBuilder() //
            .setGroupId(groupId) //
            .setPeerId(peerId.toString());
        for (final PeerId peer : newPeers) {
            rb.addNewPeers(peer.toString());
        }

        try {
            final Message result = this.cliClientService.resetPeer(peerId.getEndpoint(), rb.build(), null).get();
            return statusFromResponse(result);
        }
        catch (final Exception e) {
            return new Status(-1, e.getMessage());
        }
    }

    private void checkPeers(final Collection<PeerId> peers) {
        for (final PeerId peer : peers) {
            Requires.requireNonNull(peer, "Null peer in collection");
        }
    }

    @Override
    public Status addLearners(final String groupId, final Configuration conf, final List<PeerId> learners) {
        checkLearnersOpParams(groupId, conf, learners);

        final PeerId leaderId = new PeerId();
        final Status st = getLeader(groupId, conf, leaderId);
        if (!st.isOk()) {
            return st;
        }

        if (!this.cliClientService.connect(leaderId.getEndpoint())) {
            return new Status(-1, "Fail to init channel to leader %s", leaderId);
        }
        final AddLearnersRequest.Builder rb = AddLearnersRequest.newBuilder() //
            .setGroupId(groupId) //
            .setLeaderId(leaderId.toString());
        for (final PeerId peer : learners) {
            rb.addLearners(peer.toString());
        }

        try {
            final Message result = this.cliClientService.addLearners(leaderId.getEndpoint(), rb.build(), null).get();
            return processLearnersOpResponse(groupId, result, "adding learners: %s", learners);

        }
        catch (final Exception e) {
            return new Status(-1, e.getMessage());
        }
    }

    private void checkLearnersOpParams(final String groupId, final Configuration conf, final List<PeerId> learners) {
        Requires.requireTrue(!StringUtils.isBlank(groupId), "Blank group id");
        Requires.requireNonNull(conf, "Null configuration");
        Requires.requireTrue(learners != null && !learners.isEmpty(), "Empty peers");
        checkPeers(learners);
    }

    private Status processLearnersOpResponse(final String groupId, final Message result, final String fmt,
        final Object... formatArgs) {
        if (result instanceof LearnersOpResponse) {
            final LearnersOpResponse resp = (LearnersOpResponse) result;
            final Configuration oldConf = new Configuration();
            for (final String peerIdStr : resp.getOldLearnersList()) {
                final PeerId oldPeer = new PeerId();
                oldPeer.parse(peerIdStr);
                oldConf.addLearner(oldPeer);
            }
            final Configuration newConf = new Configuration();
            for (final String peerIdStr : resp.getNewLearnersList()) {
                final PeerId newPeer = new PeerId();
                newPeer.parse(peerIdStr);
                newConf.addLearner(newPeer);
            }

            LOG.info("Learners of replication group {} changed from {} to {} after {}.", groupId, oldConf, newConf,
                String.format(fmt, formatArgs));
            return Status.OK();
        }
        else {
            return statusFromResponse(result);
        }
    }

    @Override
    public Status removeLearners(final String groupId, final Configuration conf, final List<PeerId> learners) {
        checkLearnersOpParams(groupId, conf, learners);

        final PeerId leaderId = new PeerId();
        final Status st = getLeader(groupId, conf, leaderId);
        if (!st.isOk()) {
            return st;
        }

        if (!this.cliClientService.connect(leaderId.getEndpoint())) {
            return new Status(-1, "Fail to init channel to leader %s", leaderId);
        }
        final RemoveLearnersRequest.Builder rb = RemoveLearnersRequest.newBuilder() //
            .setGroupId(groupId) //
            .setLeaderId(leaderId.toString());
        for (final PeerId peer : learners) {
            rb.addLearners(peer.toString());
        }

        try {
            final Message result = this.cliClientService.removeLearners(leaderId.getEndpoint(), rb.build(), null).get();
            return processLearnersOpResponse(groupId, result, "removing learners: %s", learners);

        }
        catch (final Exception e) {
            return new Status(-1, e.getMessage());
        }
    }

    @Override
    public Status resetLearners(final String groupId, final Configuration conf, final List<PeerId> learners) {
        checkLearnersOpParams(groupId, conf, learners);

        final PeerId leaderId = new PeerId();
        final Status st = getLeader(groupId, conf, leaderId);
        if (!st.isOk()) {
            return st;
        }

        if (!this.cliClientService.connect(leaderId.getEndpoint())) {
            return new Status(-1, "Fail to init channel to leader %s", leaderId);
        }
        final ResetLearnersRequest.Builder rb = ResetLearnersRequest.newBuilder() //
            .setGroupId(groupId) //
            .setLeaderId(leaderId.toString());
        for (final PeerId peer : learners) {
            rb.addLearners(peer.toString());
        }

        try {
            final Message result = this.cliClientService.resetLearners(leaderId.getEndpoint(), rb.build(), null).get();
            return processLearnersOpResponse(groupId, result, "resetting learners: %s", learners);

        }
        catch (final Exception e) {
            return new Status(-1, e.getMessage());
        }
    }

    @Override
    public Status transferLeader(final String groupId, final Configuration conf, final PeerId peer) {
        Requires.requireTrue(!StringUtils.isBlank(groupId), "Blank group id");
        Requires.requireNonNull(conf, "Null configuration");
        Requires.requireNonNull(peer, "Null peer");

        final PeerId leaderId = new PeerId();
        final Status st = getLeader(groupId, conf, leaderId);
        if (!st.isOk()) {
            return st;
        }

        if (!this.cliClientService.connect(leaderId.getEndpoint())) {
            return new Status(-1, "Fail to init channel to leader %s", leaderId);
        }

        final TransferLeaderRequest.Builder rb = TransferLeaderRequest.newBuilder() //
            .setGroupId(groupId) //
            .setLeaderId(leaderId.toString());
        if (!peer.isEmpty()) {
            rb.setPeerId(peer.toString());
        }

        try {
            final Message result = this.cliClientService.transferLeader(leaderId.getEndpoint(), rb.build(), null).get();
            return statusFromResponse(result);
        }
        catch (final Exception e) {
            return new Status(-1, e.getMessage());
        }
    }

    @Override
    public Status snapshot(final String groupId, final PeerId peer) {
        Requires.requireTrue(!StringUtils.isBlank(groupId), "Blank group id");
        Requires.requireNonNull(peer, "Null peer");

        if (!this.cliClientService.connect(peer.getEndpoint())) {
            return new Status(-1, "Fail to init channel to %s", peer);
        }

        final SnapshotRequest.Builder rb = SnapshotRequest.newBuilder() //
            .setGroupId(groupId) //
            .setPeerId(peer.toString());

        try {
            final Message result = this.cliClientService.snapshot(peer.getEndpoint(), rb.build(), null).get();
            return statusFromResponse(result);
        }
        catch (final Exception e) {
            return new Status(-1, e.getMessage());
        }
    }

    @Override
    public Status getLeader(final String groupId, final Configuration conf, final PeerId leaderId) {
        Requires.requireTrue(!StringUtils.isBlank(groupId), "Blank group id");
        Requires.requireNonNull(leaderId, "Null leader id");

        if (conf == null || conf.isEmpty()) {
            return new Status(RaftError.EINVAL, "Empty group configuration");
        }

        final Status st = new Status(-1, "Fail to get leader of group %s", groupId);
        for (final PeerId peer : conf) {
            if (!this.cliClientService.connect(peer.getEndpoint())) {
                LOG.error("Fail to connect peer {} to get leader for group {}.", peer, groupId);
                continue;
            }

            final GetLeaderRequest.Builder rb = GetLeaderRequest.newBuilder() //
                .setGroupId(groupId) //
                .setPeerId(peer.toString());

            final Future<Message> result = this.cliClientService.getLeader(peer.getEndpoint(), rb.build(), null);
            try {

                final Message msg = result.get(
                    this.cliOptions.getTimeoutMs() <= 0 ? this.cliOptions.getRpcDefaultTimeout() : this.cliOptions
                        .getTimeoutMs(), TimeUnit.MILLISECONDS);
                if (msg instanceof ErrorResponse) {
                    if (st.isOk()) {
                        st.setError(-1, ((ErrorResponse) msg).getErrorMsg());
                    }
                    else {
                        final String savedMsg = st.getErrorMsg();
                        st.setError(-1, "%s, %s", savedMsg, ((ErrorResponse) msg).getErrorMsg());
                    }
                }
                else {
                    final GetLeaderResponse response = (GetLeaderResponse) msg;
                    if (leaderId.parse(response.getLeaderId())) {
                        break;
                    }
                }
            }
            catch (final Exception e) {
                if (st.isOk()) {
                    st.setError(-1, e.getMessage());
                }
                else {
                    final String savedMsg = st.getErrorMsg();

                    st.setError(-1, "%s, %s", savedMsg, e.getMessage());
                }
            }
        }

        if (leaderId.isEmpty()) {
            return st;
        }
        return Status.OK();
    }

    @Override
    public List<PeerId> getPeers(final String groupId, final Configuration conf) {
        return getPeers(groupId, conf, false, false);
    }

    @Override
    public List<PeerId> getAlivePeers(final String groupId, final Configuration conf) {
        return getPeers(groupId, conf, false, true);
    }

    @Override
    public List<PeerId> getLearners(final String groupId, final Configuration conf) {
        return getPeers(groupId, conf, true, false);
    }

    @Override
    public List<PeerId> getAliveLearners(final String groupId, final Configuration conf) {
        return getPeers(groupId, conf, true, true);
    }

    @Override
    public Status rebalance(final Set<String> balanceGroupIds, final Configuration conf,
        final Map<String, PeerId> rebalancedLeaderIds) {
        Requires.requireNonNull(balanceGroupIds, "Null balance group ids");
        Requires.requireTrue(!balanceGroupIds.isEmpty(), "Empty balance group ids");
        Requires.requireNonNull(conf, "Null configuration");
        Requires.requireTrue(!conf.isEmpty(), "No peers of configuration");

        LOG.info("Rebalance start with raft groups={}.", balanceGroupIds);

        final long start = Utils.monotonicMs();
        int transfers = 0;
        Status failedStatus = null;
        final Queue<String> groupDeque = new ArrayDeque<>(balanceGroupIds);
        final LeaderCounter leaderCounter = new LeaderCounter(balanceGroupIds.size(), conf.size());
        for (; ; ) {
            final String groupId = groupDeque.poll();
            if (groupId == null) { // well done
                break;
            }

            final PeerId leaderId = new PeerId();
            final Status leaderStatus = getLeader(groupId, conf, leaderId);
            if (!leaderStatus.isOk()) {
                failedStatus = leaderStatus;
                break;
            }

            if (rebalancedLeaderIds != null) {
                rebalancedLeaderIds.put(groupId, leaderId);
            }

            if (leaderCounter.incrementAndGet(leaderId) <= leaderCounter.getExpectedAverage()) {
                // The num of leaders is less than the expected average, we are going to deal with others
                continue;
            }

            // Find the target peer and try to transfer the leader to this peer
            final PeerId targetPeer = findTargetPeer(leaderId, groupId, conf, leaderCounter);
            if (!targetPeer.isEmpty()) {
                final Status transferStatus = transferLeader(groupId, conf, targetPeer);
                transfers++;
                if (!transferStatus.isOk()) {
                    // The failure of `transfer leader` usually means the node is busy,
                    // so we return failure status and should try `rebalance` again later.
                    failedStatus = transferStatus;
                    break;
                }

                LOG.info("Group {} transfer leader to {}.", groupId, targetPeer);
                leaderCounter.decrementAndGet(leaderId);
                groupDeque.add(groupId);
                if (rebalancedLeaderIds != null) {
                    rebalancedLeaderIds.put(groupId, targetPeer);
                }
            }
        }

        final Status status = failedStatus != null ? failedStatus : Status.OK();
        if (LOG.isInfoEnabled()) {
            LOG.info(
                "Rebalanced raft groups={}, status={}, number of transfers={}, elapsed time={} ms, rebalanced result={}.",
                balanceGroupIds, status, transfers, Utils.monotonicMs() - start, rebalancedLeaderIds);
        }
        return status;
    }

    private PeerId findTargetPeer(final PeerId self, final String groupId, final Configuration conf,
        final LeaderCounter leaderCounter) {
        for (final PeerId peerId : getAlivePeers(groupId, conf)) {
            if (peerId.equals(self)) {
                continue;
            }
            if (leaderCounter.get(peerId) >= leaderCounter.getExpectedAverage()) {
                continue;
            }
            return peerId;
        }
        return PeerId.emptyPeer();
    }

    private List<PeerId> getPeers(final String groupId, final Configuration conf, final boolean returnLearners,
        final boolean onlyGetAlive) {
        Requires.requireTrue(!StringUtils.isBlank(groupId), "Blank group id");
        Requires.requireNonNull(conf, "Null conf");

        final PeerId leaderId = new PeerId();
        final Status st = getLeader(groupId, conf, leaderId);
        if (!st.isOk()) {
            throw new IllegalStateException(st.getErrorMsg());
        }

        if (!this.cliClientService.connect(leaderId.getEndpoint())) {
            throw new IllegalStateException("Fail to init channel to leader " + leaderId);
        }

        final GetPeersRequest.Builder rb = GetPeersRequest.newBuilder() //
            .setGroupId(groupId) //
            .setLeaderId(leaderId.toString()) //
            .setOnlyAlive(onlyGetAlive);

        try {
            final Message result = this.cliClientService.getPeers(leaderId.getEndpoint(), rb.build(), null).get(
                this.cliOptions.getTimeoutMs() <= 0 ? this.cliOptions.getRpcDefaultTimeout()
                    : this.cliOptions.getTimeoutMs(), TimeUnit.MILLISECONDS);
            if (result instanceof GetPeersResponse) {
                final GetPeersResponse resp = (GetPeersResponse) result;
                final List<PeerId> peerIdList = new ArrayList<>();
                final List<String> responsePeers = returnLearners ? resp.getLearnersList() : resp.getPeersList();
                for (final String peerIdStr : responsePeers) {
                    final PeerId newPeer = new PeerId();
                    newPeer.parse(peerIdStr);
                    peerIdList.add(newPeer);
                }
                return peerIdList;
            }
            else {
                final ErrorResponse resp = (ErrorResponse) result;
                throw new JRaftException(resp.getErrorMsg());
            }
        }
        catch (final JRaftException e) {
            throw e;
        }
        catch (final Exception e) {
            throw new JRaftException(e);
        }
    }

    public CliClientService getCliClientService() {
        return this.cliClientService;
    }

    private static class LeaderCounter {

        private final Map<PeerId, Integer> counter = new HashMap<>();
        // The expected average leader number on every peerId
        private final int expectedAverage;

        LeaderCounter(final int groupCount, final int peerCount) {
            this.expectedAverage = (int) Math.ceil((double) groupCount / peerCount);
        }

        public int getExpectedAverage() {
            return this.expectedAverage;
        }

        public int incrementAndGet(final PeerId peerId) {
            return this.counter.compute(peerId, (ignored, num) -> num == null ? 1 : num + 1);
        }

        public int decrementAndGet(final PeerId peerId) {
            return this.counter.compute(peerId, (ignored, num) -> num == null ? 0 : num - 1);
        }

        public int get(final PeerId peerId) {
            return this.counter.getOrDefault(peerId, 0);
        }
    }
}
