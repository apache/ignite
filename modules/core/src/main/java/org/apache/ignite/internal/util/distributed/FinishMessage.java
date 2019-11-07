package org.apache.ignite.internal.util.distributed;

import java.util.UUID;
import org.apache.ignite.internal.managers.discovery.DiscoveryCustomMessage;

/**
 * Action message interface of the {@link DistributedProcessManager}.
 */
public interface FinishMessage extends DiscoveryCustomMessage {
    /** @return Request id. */
    public UUID requestId();
}
