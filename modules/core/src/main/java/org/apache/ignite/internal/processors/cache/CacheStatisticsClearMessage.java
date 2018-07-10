package org.apache.ignite.internal.processors.cache;

import java.util.Collection;
import java.util.UUID;
import org.apache.ignite.internal.managers.discovery.DiscoveryCustomMessage;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.jetbrains.annotations.Nullable;

/**
 * Cache statistics clear discovery message.
 */
public class CacheStatisticsClearMessage extends CacheStatisticsManageMessage {
    /** */
    private static final long serialVersionUID = 0L;

    /**
     * Constructor for request.
     *
     * @param caches Collection of cache names.
     */
    public CacheStatisticsClearMessage(UUID reqId, Collection<String> caches) {
        super(reqId, caches, (byte)0);
    }

    /**
     * Constructor for response.
     *
     * @param msg Request message.
     */
    private CacheStatisticsClearMessage(CacheStatisticsManageMessage msg) {
        super(msg);
    }

    /** {@inheritDoc} */
    @Nullable @Override public DiscoveryCustomMessage ackMessage() {
        return initial() ? new CacheStatisticsClearMessage(this) : null;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(CacheStatisticsClearMessage.class, this);
    }
}
