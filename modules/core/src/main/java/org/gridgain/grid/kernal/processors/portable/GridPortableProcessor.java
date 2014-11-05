/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.portable;

import org.gridgain.client.marshaller.*;
import org.gridgain.grid.kernal.processors.*;
import org.gridgain.grid.portables.*;
import org.gridgain.grid.product.*;
import org.jetbrains.annotations.*;

import java.nio.*;
import java.util.*;

/**
 * Portable processor.
 */
public interface GridPortableProcessor extends GridProcessor {
    /** */
    public static final GridProductVersion SINCE_VER = GridProductVersion.fromString("6.2.0");

    /**
     * @param typeName Type name.
     * @return Type ID.
     */
    public int typeId(String typeName);

    /**
     * @param obj Object to marshal.
     * @param trim If {@code true} trims result byte buffer.
     * @return Object bytes.
     * @throws GridPortableException In case of error.
     */
    public ByteBuffer marshal(@Nullable Object obj, boolean trim) throws GridPortableException;

    /**
     * @param arr Byte array.
     * @param off Offset.
     * @return Unmarshalled object.
     * @throws GridPortableException In case of error.
     */
    public Object unmarshal(byte[] arr, int off) throws GridPortableException;

    /**
     * @param ptr Offheap pointer.
     * @param forceHeap If {@code true} creates heap-based object.
     * @return Unmarshalled object.
     * @throws GridPortableException In case of error.
     */
    Object unmarshal(long ptr, boolean forceHeap) throws GridPortableException;

    /**
     * Converts temporary offheap object to heap-based.
     *
     * @param obj Object.
     * @return Heap-based object.
     * @throws GridPortableException In case of error.
     */
    @Nullable Object unwrapTemporary(@Nullable Object obj) throws GridPortableException;

    /**
     * @param obj Object to marshal.
     * @return Portable object.
     * @throws GridPortableException In case of error.
     */
    public Object marshalToPortable(@Nullable Object obj) throws GridPortableException;

    /**
     * @param obj Object (portable or not).
     * @return Detached portable object or original object.
     */
    public Object detachPortable(@Nullable Object obj);

    /**
     * @return Portable marshaller for client connectivity or {@code null} if it's not
     *      supported (in case of OS edition).
     */
    @Nullable public GridClientMarshaller portableMarshaller();

    /**
     * @param marsh Client marshaller.
     * @return Whether marshaller is portable.
     */
    public boolean isPortable(GridClientMarshaller marsh);

    /**
     * @return Builder.
     */
    public GridPortableBuilder builder();

    /**
     * Gets mutable portable object to edit existing portable object.
     *
     * @param portableObj Portable object to edit.
     * @return Portable builder.
     */
    public GridMutablePortable toMutable(GridPortableObject portableObj);

    /**
     * @param typeId Type ID.
     * @param newMeta New meta data.
     * @throws GridPortableException In case of error.
     */
    public void addMeta(int typeId, final GridPortableMetadata newMeta) throws GridPortableException;

    /**
     * @param typeId Type ID.
     * @param typeName Type name.
     * @param affKeyFieldName Affinity key field name.
     * @param fieldTypeIds Fields map.
     * @throws GridPortableException In case of error.
     */
    void updateMetaData(int typeId, String typeName, @Nullable String affKeyFieldName,
        Map<String, Integer> fieldTypeIds) throws GridPortableException;

    /**
     * @param typeId Type ID.
     * @return Meta data.
     * @throws GridPortableException In case of error.
     */
    @Nullable public GridPortableMetadata metadata(int typeId) throws GridPortableException;

    /**
     * @param typeIds Type ID.
     * @return Meta data.
     * @throws GridPortableException In case of error.
     */
    public Map<Integer, GridPortableMetadata> metadata(Collection<Integer> typeIds) throws GridPortableException;

    /**
     * @return Metadata for all types.
     * @throws GridPortableException In case of error.
     */
    public Collection<GridPortableMetadata> metadata() throws GridPortableException;
}
