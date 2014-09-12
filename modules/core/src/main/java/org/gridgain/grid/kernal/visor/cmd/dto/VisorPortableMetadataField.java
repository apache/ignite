/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.visor.cmd.dto;

import org.gridgain.grid.util.typedef.internal.*;

import java.io.*;

/**
 * Portable object metadata field information.
 */
public class VisorPortableMetadataField implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Field name. */
    private String fieldName;

    /** Field type name. */
    private String fieldTypeName;

    /** Field id. */
    private Integer fieldId;

    /** Field name. */
    public String fieldName() {
        return fieldName;
    }

    public void fieldName(String fieldName) {
        this.fieldName = fieldName;
    }

    /** Field type name. */
    public String fieldTypeName() {
        return fieldTypeName;
    }

    public void fieldTypeName(String fieldTypeName) {
        this.fieldTypeName = fieldTypeName;
    }

    /** Field id. */
    public Integer fieldId() {
        return fieldId;
    }

    public void fieldId(Integer fieldId) {
        this.fieldId = fieldId;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(VisorPortableMetadataField.class, this);
    }
}
