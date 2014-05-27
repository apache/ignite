package org.gridgain.grid.kernal.visor.cmd.dto.node;

/**
 * Created by vsisko on 5/27/14.
 */
public class VisorFieldsQueryColumn {
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
