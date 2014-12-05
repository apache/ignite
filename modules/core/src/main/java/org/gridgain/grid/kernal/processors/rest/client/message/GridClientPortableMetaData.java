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
 * Portable meta data sent from client.
 */
public class GridClientPortableMetaData implements PortableMarshalAware {
    /** */
    private int typeId;

    /** */
    private String typeName;

    /** */
    private Map<String, Integer> fields;

    /** */
    private String affKeyFieldName;

    /**
     * @return Type ID.
     */
    public int typeId() {
        return typeId;
    }

    /**
     * @return Type name.
     */
    public String typeName() {
        return typeName;
    }

    /**
     * @return Fields.
     */
    public Map<String, Integer> fields() {
        return fields;
    }

    /**
     * @return Affinity key field name.
     */
    public String affinityKeyFieldName() {
        return affKeyFieldName;
    }

    /** {@inheritDoc} */
    @Override public void writePortable(PortableWriter writer) throws PortableException {
        PortableRawWriter raw = writer.rawWriter();

        raw.writeInt(typeId);
        raw.writeString(typeName);
        raw.writeString(affKeyFieldName);
        raw.writeMap(fields);
    }

    /** {@inheritDoc} */
    @Override public void readPortable(PortableReader reader) throws PortableException {
        PortableRawReader raw = reader.rawReader();

        typeId = raw.readInt();
        typeName = raw.readString();
        affKeyFieldName = raw.readString();
        fields = raw.readMap();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridClientPortableMetaData.class, this);
    }
}
