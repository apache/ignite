package org.apache.ignite.internal.util.distributed;

import java.util.UUID;
import org.apache.ignite.internal.managers.discovery.DiscoveryCustomMessage;

/**
 * Initial message interface of the {@link DistributedProcess}.
 */
public interface DistributedProcessInitialMessage extends DiscoveryCustomMessage {
    /** @return Request id. */
    public UUID requestId();
}
