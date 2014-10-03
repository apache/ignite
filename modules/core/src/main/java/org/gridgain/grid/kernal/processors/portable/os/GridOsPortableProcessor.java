/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.portable.os;

import org.gridgain.client.marshaller.*;
import org.gridgain.grid.kernal.*;
import org.gridgain.grid.kernal.processors.*;
import org.gridgain.grid.kernal.processors.portable.*;
import org.gridgain.grid.portables.*;
import org.jetbrains.annotations.*;

import java.nio.*;
import java.util.*;

/**
 * No-op implementation of {@link GridPortableProcessor}.
 */
public class GridOsPortableProcessor extends GridProcessorAdapter implements GridPortableProcessor {
    /**
     * @param ctx Kernal context.
     */
    public GridOsPortableProcessor(GridKernalContext ctx) {
        super(ctx);
    }

    /** {@inheritDoc} */
    @Override public int typeId(String typeName) {
        return 0;
    }

    /** {@inheritDoc} */
    @Override public ByteBuffer marshal(@Nullable Object obj, boolean trim) throws GridPortableException {
        return null;
    }

    /** {@inheritDoc} */
    @Nullable @Override public Object unmarshal(byte[] arr) throws GridPortableException {
        return null;
    }

    /** {@inheritDoc} */
    @Override public Object unmarshal(long ptr, boolean forceHeap) throws GridPortableException {
        return null;
    }

    /** {@inheritDoc} */
    @Override public Object heapObject(Object obj) throws GridPortableException {
        return null;
    }

    /** {@inheritDoc} */
    @Nullable @Override public Object marshalToPortable(@Nullable Object obj) throws GridPortableException {
        return obj;
    }

    /** {@inheritDoc} */
    @Override public Object detachPortable(@Nullable Object obj) {
        return obj;
    }

    /** {@inheritDoc} */
    @Nullable @Override public GridClientMarshaller portableMarshaller() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public boolean isPortable(GridClientMarshaller marsh) {
        return false;
    }

    /** {@inheritDoc} */
    @Override public GridPortableBuilder builder() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public void addMeta(int typeId, GridPortableMetadata newMeta) throws GridPortableException {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void updateMetaData(int typeId, String typeName, String affKeyFieldName,
        Map<String, Integer> fieldTypeIds) throws GridPortableException {
        // No-op.
    }

    /** {@inheritDoc} */
    @Nullable @Override public GridPortableMetadata metadata(int typeId) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public Map<Integer, GridPortableMetadata> metadata(Collection<Integer> typeIds) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public Collection<GridPortableMetadata> metadata() throws GridPortableException {
        return Collections.emptyList();
    }
}
