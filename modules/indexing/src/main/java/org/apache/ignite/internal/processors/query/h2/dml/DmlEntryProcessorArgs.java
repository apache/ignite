package org.apache.ignite.internal.processors.query.h2.dml;

import java.io.Serializable;
import java.util.LinkedHashMap;

/**
 * Arguments for {@link EntryModifier}.
 */
public final class DmlEntryProcessorArgs implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /**
     * Target type descriptor name.
     */
    public final String typeName;

    /**
     * Same as {@link UpdatePlan#props} - map from property indexes in order defined by type descriptor
     * to their positions in array of new property values.
     */
    public final LinkedHashMap<Integer, Integer> props;

    /** */
    public DmlEntryProcessorArgs(String typeName, LinkedHashMap<Integer, Integer> props) {
        this.typeName = typeName;
        this.props = props;
    }
}
