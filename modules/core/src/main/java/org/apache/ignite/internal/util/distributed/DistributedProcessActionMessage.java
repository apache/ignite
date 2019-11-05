package org.apache.ignite.internal.util.distributed;

import java.util.UUID;
import org.apache.ignite.internal.managers.discovery.DiscoveryCustomMessage;

/**
 * Action message interface of the {@link DistributedProcess}.
 */
public interface DistributedProcessActionMessage extends DiscoveryCustomMessage {
    /** @return Request id. */
    public UUID requestId();
}
