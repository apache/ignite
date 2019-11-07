package org.apache.ignite.internal.util.distributed;

import java.util.Map;
import java.util.UUID;
import org.apache.ignite.internal.IgniteInternalFuture;

/** */
public interface DistributedProcess<I extends InitMessage, R extends SingleNodeMessage, F extends FinishMessage> {
    /**
     * Processes initial discovery message. Called on each server node.
     * <p>
     * Note: called from discovery thread.
     *
     * @param msg Initial discovery message {@link InitMessage}.
     * @return Future with single nodes result {@link SingleNodeMessage}.
     */
    IgniteInternalFuture<R> execute(I msg);

    /** */
    F buildFinishMessage(Map<UUID, R> singleMsgs);

    /** */
    void finish(F msg);
}
