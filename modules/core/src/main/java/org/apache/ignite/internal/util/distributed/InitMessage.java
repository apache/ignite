package org.apache.ignite.internal.util.distributed;

import java.util.UUID;
import org.apache.ignite.internal.managers.discovery.DiscoveryCustomMessage;

/**
 * Initial message interface of the {@link DistributedProcessManager}.
 */
public interface InitMessage extends DiscoveryCustomMessage {
    /** @return Request id. */
    public UUID requestId();
}
