/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.visor.cmd.dto.node;

import java.io.Serializable;

/**
 * Visor query column type description.
 */
public class VisorFieldsQueryColumn implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    private final String type;

    private final String field;

    public VisorFieldsQueryColumn(String type, String field) {
        this.type = type;
        this.field = field;
    }

    /**
     * @return Type.
     */
    public String type() {
        return type;
    }

    /**
     * @return Field.
     */
    public String field() {
        return field;
    }
}
