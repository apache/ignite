package org.apache.ignite.internal.processors.platform.client.beforestart;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.binary.BinaryRawReader;
import org.apache.ignite.internal.processors.platform.client.ClientConnectionContext;
import org.apache.ignite.internal.processors.platform.client.ClientRequest;
import org.apache.ignite.internal.processors.platform.client.ClientResponse;

/**
 * Stop warmup request.
 */
public class ClientCacheStopWarmupRequest extends ClientRequest implements BeforeStartupRequest {
    /**
     * Constructor.
     *
     * @param reader Reader.
     */
    public ClientCacheStopWarmupRequest(BinaryRawReader reader) {
        super(reader);
    }

    /** {@inheritDoc} */
    @Override public ClientResponse process(ClientConnectionContext ctx) {
        try {
            ctx.kernalContext().cache().stopWarmUp();
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException(e);
        }

        return super.process(ctx);
    }
}
