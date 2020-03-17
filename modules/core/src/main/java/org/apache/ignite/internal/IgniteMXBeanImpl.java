package org.apache.ignite.internal;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import javax.management.JMException;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.mxbean.IgniteMXBean;

import static org.apache.ignite.cluster.ClusterState.ACTIVE;
import static org.apache.ignite.cluster.ClusterState.INACTIVE;
import static org.apache.ignite.internal.IgniteVersionUtils.COPYRIGHT;

/** Implementation of Ignite mxBean. */
public class IgniteMXBeanImpl implements IgniteMXBean {
    /** Ignite core to work with. */
    private final IgniteKernal kernal;

    /** Ignite kernal context. */
    private final GridKernalContext ctx;

    /**
     * @param kernal Ignite kernel to work with.
     */
    public IgniteMXBeanImpl(IgniteKernal kernal) {
        this.kernal = kernal;

        this.ctx = kernal.context();
    }

    /** {@inheritDoc} */
    @Override public boolean active() {
        return kernal.active();
    }

    /** {@inheritDoc} */
    @Override public void active(boolean active) {
        clusterState(active ? ACTIVE.toString() : INACTIVE.toString(), false);
    }

    /** {@inheritDoc} */
    @Override public String getCopyright() {
        return COPYRIGHT;
    }

    /** {@inheritDoc} */
    @Override public long getStartTimestamp() {
        return kernal.startTimestamp();
    }

    /** {@inheritDoc} */
    @Override public String getStartTimestampFormatted() {
        return kernal.startTimeFormatted();
    }

    /** {@inheritDoc} */
    @Override public long getUpTime() {
        return kernal.upTime();
    }

    /** {@inheritDoc} */
    @Override public long getLongJVMPausesCount() {
        return kernal.longJVMPausesCount();
    }

    /** {@inheritDoc} */
    @Override public long getLongJVMPausesTotalDuration() {
        return kernal.longJVMPausesTotalDuration();
    }

    /** {@inheritDoc} */
    @Override public Map<Long, Long> getLongJVMPauseLastEvents() {
        return kernal.longJVMPauseLastEvents();
    }

    /** {@inheritDoc} */
    @Override public String getUpTimeFormatted() {
        return kernal.upTimeFormatted();
    }

    /** {@inheritDoc} */
    @Override public String getFullVersion() {
        return kernal.fullVersion();
    }

    /** {@inheritDoc} */
    @Override public String getCheckpointSpiFormatted() {
        return kernal.checkpointSpiFormatted();
    }

    /** {@inheritDoc} */
    @Override public String getCurrentCoordinatorFormatted() {
        return kernal.currentCoordinatorFormatted();
    }

    /** {@inheritDoc} */
    @Override public boolean isNodeInBaseline() {
       return kernal.nodeInBaseline();
    }

    /** {@inheritDoc} */
    @Override public String getCommunicationSpiFormatted() {
        return kernal.communicationSpiFormatted();
    }

    /** {@inheritDoc} */
    @Override public String getDeploymentSpiFormatted() {
        return kernal.deploymentSpiFormatted();
    }

    /** {@inheritDoc} */
    @Override public String getDiscoverySpiFormatted() {
        return kernal.discoverySpiFormatted();
    }

    /** {@inheritDoc} */
    @Override public String getEventStorageSpiFormatted() {
        return kernal.eventStorageSpiFormatted();
    }

    /** {@inheritDoc} */
    @Override public String getCollisionSpiFormatted() {
        return kernal.collisionSpiFormatted();
    }

    /** {@inheritDoc} */
    @Override public String getFailoverSpiFormatted() {
        return kernal.failoverSpiFormatted();
    }

    /** {@inheritDoc} */
    @Override public String getLoadBalancingSpiFormatted() {
        return kernal.loadBalancingSpiFormatted();
    }

    /** {@inheritDoc} */
    @Override public String getOsInformation() {
        return kernal.osInformation();
    }

    /** {@inheritDoc} */
    @Override public String getJdkInformation() {
        return kernal.jdkInformation();
    }

    /** {@inheritDoc} */
    @Override public String getOsUser() {
        return kernal.osUser();
    }

    /** {@inheritDoc} */
    @Override public void printLastErrors() {
        kernal.printLastErrors();
    }

    /** {@inheritDoc} */
    @Override public String getVmName() {
        return kernal.vmName();
    }

    /** {@inheritDoc} */
    @Override public String getInstanceName() {
        return kernal.name();
    }

    /** {@inheritDoc} */
    @Override public String getExecutorServiceFormatted() {
        return kernal.executorServiceFormatted();
    }

    /** {@inheritDoc} */
    @Override public String getIgniteHome() {
        return kernal.igniteHome();
    }

    /** {@inheritDoc} */
    @Override public String getGridLoggerFormatted() {
        return kernal.gridLoggerFormatted();
    }

    /** {@inheritDoc} */
    @Override public String getMBeanServerFormatted() {
        return kernal.mbeanServerFormatted();
    }

    /** {@inheritDoc} */
    @Override public UUID getLocalNodeId() {
        return kernal.localNodeId();
    }

    /** {@inheritDoc} */
    @Override public List<String> getUserAttributesFormatted() {
        return kernal.userAttributesFormatted();
    }

    /** {@inheritDoc} */
    @Override public boolean isPeerClassLoadingEnabled() {
        return kernal.peerClassLoadingEnabled();
    }

    /** {@inheritDoc} */
    @Override public List<String> getLifecycleBeansFormatted() {
        return kernal.lifecycleBeansFormatted();
    }

    /** {@inheritDoc} */
    @Override public String clusterState() {
        return kernal.clusterState();
    }

    /** {@inheritDoc} */
    @Override public long lastClusterStateChangeTime() {
        return kernal.lastClusterStateChangeTime();
    }

    /** {@inheritDoc} */
    @Override public boolean removeCheckpoint(String key) {
        A.notNull(key, "key");

        return kernal.removeCheckpoint(key);
    }

    /** {@inheritDoc} */
    @Override public boolean pingNode(String nodeId) {
        A.notNull(nodeId, "nodeId");

        return kernal.cluster().pingNode(UUID.fromString(nodeId));
    }

    /** {@inheritDoc} */
    @Override public void undeployTaskFromGrid(String taskName) throws JMException {
        A.notNull(taskName, "taskName");

        try {
            kernal.compute().undeployTask(taskName);
        }
        catch (IgniteException e) {
            throw U.jmException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public String executeTask(String taskName, String arg) throws JMException {
        try {
            return kernal.compute().execute(taskName, arg);
        }
        catch (IgniteException e) {
            throw U.jmException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public boolean pingNodeByAddress(String host) {
        ctx.gateway().readLock();

        try {
            for (ClusterNode n : kernal.cluster().nodes())
                if (n.addresses().contains(host))
                    return ctx.discovery().pingNode(n.id());

            return false;
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
        finally {
            ctx.gateway().readUnlock();
        }
    }

    /** {@inheritDoc} */
    @Override public void dumpDebugInfo() {
        kernal.dumpDebugInfo();
    }

    /** {@inheritDoc} */
    @Override public void runIoTest(
        long warmup,
        long duration,
        int threads,
        long maxLatency,
        int rangesCnt,
        int payLoadSize,
        boolean procFromNioThread
    ) {
        ctx.io().runIoTest(warmup, duration, threads, maxLatency, rangesCnt, payLoadSize,
            procFromNioThread, new ArrayList(ctx.cluster().get().forServers().forRemotes().nodes()));
    }

    /** {@inheritDoc} */
    @Override public void clearNodeLocalMap() {
        ctx.cluster().get().clearNodeMap();
    }

    /** {@inheritDoc} */
    @Override public void clusterState(String state) {
        clusterState(state, false);
    }

    /** {@inheritDoc} */
    @Override public void clusterState(String state, boolean forceDeactivation) {
        ClusterState newState = ClusterState.valueOf(state);

        ctx.gateway().readLock();

        try {
            ctx.state().changeGlobalState(newState, forceDeactivation, ctx.cluster().get()
                .forServers().nodes(), false).get();
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
        finally {
            ctx.gateway().readUnlock();
        }
    }

    /** {@inheritDoc} */
    @Override public boolean isRebalanceEnabled() {
        return kernal.isRebalanceEnabled();
    }

    /** {@inheritDoc} */
    @Override public void rebalanceEnabled(boolean rebalanceEnabled) {
        kernal.rebalanceEnabled(rebalanceEnabled);
    }
}
