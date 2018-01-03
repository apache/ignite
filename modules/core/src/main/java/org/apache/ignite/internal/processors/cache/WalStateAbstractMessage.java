package org.apache.ignite.internal.processors.cache;

import org.apache.ignite.internal.managers.discovery.DiscoCache;
import org.apache.ignite.internal.managers.discovery.DiscoveryCustomMessage;
import org.apache.ignite.internal.managers.discovery.GridDiscoveryManager;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.lang.IgniteUuid;
import org.jetbrains.annotations.Nullable;

import java.util.UUID;

/**
 * WAL state change abstract message.
 */
public abstract class WalStateAbstractMessage implements DiscoveryCustomMessage {
    /** Message ID */
    private final IgniteUuid id = IgniteUuid.randomUuid();

    /** Unique operation ID. */
    private final UUID opId;

    /** Group ID. */
    private int grpId;

    /** Group deployment ID. */
    private IgniteUuid grpDepId;

    /** Message that should be processed through exchange thread. */
    private transient WalStateProposeMessage exchangeMsg;

    /**
     * Constructor.
     *
     * @param opId Unique operation ID.
     * @param grpId Group ID.
     * @param grpDepId Group deployment ID.
     */
    protected WalStateAbstractMessage(UUID opId, int grpId, IgniteUuid grpDepId) {
        this.opId = opId;
        this.grpId = grpId;
        this.grpDepId = grpDepId;
    }

    /**
     * @return Unique operation ID.
     */
    public UUID operationId() {
        return opId;
    }

    /**
     * @return Group ID.
     */
    public int groupId() {
        return grpId;
    }

    /**
     * @return Group deployment ID.
     */
    public IgniteUuid groupDeploymentId() {
        return grpDepId;
    }

    /**
     * Get exchange message.
     *
     * @return Massage or {@code null} if no processing is required.
     */
    @Nullable public WalStateProposeMessage exchangeMessage() {
        return exchangeMsg;
    }

    /**
     * Set message that will be processed through exchange thread later on.
     *
     * @param exchangeMsg Message.
     */
    public void exchangeMessage(WalStateProposeMessage exchangeMsg) {
        this.exchangeMsg = exchangeMsg;
    }

    /** {@inheritDoc} */
    @Override public IgniteUuid id() {
        return id;
    }

    /** {@inheritDoc} */
    @Nullable @Override public DiscoveryCustomMessage ackMessage() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public boolean isMutable() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public DiscoCache createDiscoCache(GridDiscoveryManager mgr, AffinityTopologyVersion topVer,
        DiscoCache discoCache) {
        return null;
    }
}
