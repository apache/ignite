/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.util.tostring;

import java.util.*;

/**
 * Simple class descriptor containing simple and fully qualified class names as well as
 * the list of class fields.
 */
class GridToStringClassDescriptor {
    /** */
    private final String sqn;

    /** */
    private final String fqn;

    /** */
    private List<GridToStringFieldDescriptor> fields = new ArrayList<>();

    /**
     * @param cls Class.
     */
    GridToStringClassDescriptor(Class<?> cls) {
        assert cls != null;

        fqn = cls.getName();
        sqn = cls.getSimpleName();
    }

    /**
     * @param field Field descriptor to be added.
     */
    void addField(GridToStringFieldDescriptor field) {
        assert field != null;

        fields.add(field);
    }

    /** */
    void sortFields() {
        Collections.sort(fields, new Comparator<GridToStringFieldDescriptor>() {
            /** {@inheritDoc} */
            @Override public int compare(GridToStringFieldDescriptor arg0, GridToStringFieldDescriptor arg1) {
                return arg0.getOrder() < arg1.getOrder() ? -1 : arg0.getOrder() > arg1.getOrder() ? 1 : 0;
            }
        });
    }

    /**
     * @return Simple class name.
     */
    String getSimpleClassName() {
        return sqn;
    }

    /**
     * @return Fully qualified class name.
     */
    String getFullyQualifiedClassName() {
        return fqn;
    }

    /**
     * @return List of fields.
     */
    List<GridToStringFieldDescriptor> getFields() {
        return fields;
    }
}
