/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.client.marshaller.portable;

import org.gridgain.client.*;

import java.util.*;

/**
 * Portable class metadata.
 */
class GridPortableClassMetadata {
    /** */
    private int typeId;

    /** */
    private Class<? extends GridPortableObject> cls;

    /** */
    private List<String> fields;

    /**
     * @param typeId Type identified.
     * @param cls Target java class.
     * @param fields Field names.
     */
    GridPortableClassMetadata(int typeId, Class<? extends GridPortableObject> cls, List<String> fields) {
        this.typeId = typeId;
        this.cls = cls;
        this.fields = fields;
    }

    /**
     * @return Type identified.
     */
    public int typeId() {
        return typeId;
    }

    /**
     * @return Java class.
     */
    public Class<? extends GridPortableObject> targetClass() {
        return cls;
    }

    /**
     * @return Field names.
     */
    public List<String> fields() {
        return fields;
    }
}
