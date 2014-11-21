/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.visor.dto;

import org.gridgain.grid.util.typedef.internal.*;

import java.io.*;

/**
 * Data transfer object for query column type description.
 */
public class VisorFieldsQueryColumn implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Column type. */
    private final String type;

    /** Field name. */
    private final String field;

    /**
     * Create data transfer object with given parameters.
     *
     * @param type Column type.
     * @param field Field name.
     */
    public VisorFieldsQueryColumn(String type, String field) {
        this.type = type;
        this.field = field;
    }

    /**
     * @return Column type.
     */
    public String type() {
        return type;
    }

    /**
     * @return Field name.
     */
    public String field() {
        return field;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(VisorFieldsQueryColumn.class, this);
    }
}
