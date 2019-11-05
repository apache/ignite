package org.apache.ignite.internal.util.distributed;

import java.util.UUID;
import org.apache.ignite.plugin.extensions.communication.Message;

/**
 * Single node result message interface of the {@link DistributedProcess}.
 */
public interface DistributedProcessSingleNodeMessage extends Message {
    /** @return Request id. */
    public UUID requestId();
}
