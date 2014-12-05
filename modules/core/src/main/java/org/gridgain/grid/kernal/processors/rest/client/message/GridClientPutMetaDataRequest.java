/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.rest.client.message;

import org.apache.ignite.portables.*;
import org.gridgain.grid.util.typedef.internal.*;

import java.util.*;

/**
 * Metadata put request.
 */
public class GridClientPutMetaDataRequest extends GridClientAbstractMessage {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private Collection<GridClientPortableMetaData> meta;

    /**
     * @return Type IDs.
     */
    public Collection<GridClientPortableMetaData> metaData() {
        return meta;
    }

    /** {@inheritDoc} */
    @Override public void writePortable(GridPortableWriter writer) throws PortableException {
        super.writePortable(writer);

        GridPortableRawWriter raw = writer.rawWriter();

        raw.writeCollection(meta);
    }

    /** {@inheritDoc} */
    @Override public void readPortable(GridPortableReader reader) throws PortableException {
        super.readPortable(reader);

        GridPortableRawReader raw = reader.rawReader();

        meta = raw.readCollection();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridClientPutMetaDataRequest.class, this);
    }
}
