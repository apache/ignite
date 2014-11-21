/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.visor.dto.portable;

import org.gridgain.grid.util.typedef.internal.*;

import java.io.*;
import java.util.*;

/**
 * Portable object metadata to show in Visor.
 */
public class VisorPortableMetadata implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Type name */
    private String typeName;

    /** Type Id */
    private Integer typeId;

    /** Filed list */
    private Collection<VisorPortableMetadataField> fields;

    /** Type name */
    public String typeName() {
        return typeName;
    }

    public void typeName(String typeName) {
        this.typeName = typeName;
    }

    /** Type Id */
    public Integer typeId() {
        return typeId;
    }

    public void typeId(Integer typeId) {
        this.typeId = typeId;
    }

    /** Fields list */
    public Collection<VisorPortableMetadataField> fields() {
        return fields;
    }

    public void fields(Collection<VisorPortableMetadataField> fields) {
        this.fields = fields;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(VisorPortableMetadata.class, this);
    }
}
