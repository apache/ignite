/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.rest.request;

import org.gridgain.grid.kernal.processors.rest.client.message.*;

import java.util.*;

/**
 * Portable metadata request.
 */
public class GridRestPortableMetaDataRequest extends GridRestRequest {
    /** */
    private final GridClientMetaDataRequest msg;

    /**
     * @param msg Client message.
     */
    public GridRestPortableMetaDataRequest(GridClientMetaDataRequest msg) {
        this.msg = msg;
    }

    /**
     * @return Type IDs.
     */
    public Collection<Integer> typeIds() {
        return msg.typeIds();
    }
}
