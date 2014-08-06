/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.rest.handlers.metadata;

import org.gridgain.grid.*;
import org.gridgain.grid.kernal.*;
import org.gridgain.grid.kernal.processors.rest.*;
import org.gridgain.grid.kernal.processors.rest.client.message.*;
import org.gridgain.grid.kernal.processors.rest.handlers.*;
import org.gridgain.grid.kernal.processors.rest.request.*;
import org.gridgain.grid.portables.*;
import org.gridgain.grid.util.future.*;
import org.gridgain.grid.util.typedef.internal.*;

import java.util.*;

import static org.gridgain.grid.kernal.processors.rest.GridRestCommand.*;

/**
 * Portable metadata handler.
 */
public class GridPortableMetadataHandler extends GridRestCommandHandlerAdapter {
    /** Supported commands. */
    private static final Collection<GridRestCommand> SUPPORTED_COMMANDS = U.sealList(
        GET_PORTABLE_METADATA
    );

    /**
     * @param ctx Context.
     */
    public GridPortableMetadataHandler(GridKernalContext ctx) {
        super(ctx);
    }

    /** {@inheritDoc} */
    @Override public Collection<GridRestCommand> supportedCommands() {
        return SUPPORTED_COMMANDS;
    }

    /** {@inheritDoc} */
    @Override public GridFuture<GridRestResponse> handleAsync(GridRestRequest req) {
        assert(req.command() == GET_PORTABLE_METADATA);

        try {
            GridRestPortableMetaDataRequest metaReq = (GridRestPortableMetaDataRequest)req;

            GridRestResponse res = new GridRestResponse();

            Map<Integer, GridPortableMetaData> meta = ctx.portable().metaData(metaReq.typeIds());

            GridClientMetaDataResponse metaRes = new GridClientMetaDataResponse();

            metaRes.metaData(meta);

            res.setResponse(metaRes);

            return new GridFinishedFuture<>(ctx, res);
        }
        catch (GridException e) {
            return new GridFinishedFuture<>(ctx, e);
        }
    }
}
