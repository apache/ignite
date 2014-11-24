/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.visor.query;

import org.gridgain.grid.util.typedef.internal.*;

import java.io.*;

/**
 * Data transfer object for query field type description.
 */
public class VisorQueryField implements Serializable {
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
    public VisorQueryField(String type, String field) {
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
        return S.toString(VisorQueryField.class, this);
    }
}
