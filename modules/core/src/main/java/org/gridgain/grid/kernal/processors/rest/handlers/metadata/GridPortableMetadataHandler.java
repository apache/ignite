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
        PUT_PORTABLE_METADATA,
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
        assert SUPPORTED_COMMANDS.contains(req.command()) : req.command();

        try {
            if (req.command() == GET_PORTABLE_METADATA) {
                GridRestPortableGetMetaDataRequest metaReq = (GridRestPortableGetMetaDataRequest)req;

                GridRestResponse res = new GridRestResponse();

                Map<Integer, GridPortableMetadata> meta = ctx.portable().metaData(metaReq.typeIds());

                GridClientMetaDataResponse metaRes = new GridClientMetaDataResponse();

                metaRes.metaData(meta);

                res.setResponse(metaRes);

                return new GridFinishedFuture<>(ctx, res);
            }
            else {
                assert req.command() == PUT_PORTABLE_METADATA;

                GridRestPortablePutMetaDataRequest metaReq = (GridRestPortablePutMetaDataRequest)req;

                for (GridClientPortableMetaData meta : metaReq.metaData())
                    ctx.portable().updateMetaData(meta.typeId(),
                        meta.typeName(),
                        meta.affinityKeyFieldName(),
                        meta.fields());

                GridRestResponse res = new GridRestResponse();

                res.setResponse(true);

                return new GridFinishedFuture<>(ctx, res);
            }
        }
        catch (GridRuntimeException e) {
            return new GridFinishedFuture<>(ctx, e);
        }
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridPortableMetadataHandler.class, this, super.toString());
    }
}
