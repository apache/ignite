/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.util.tostring;

/**
 * Simple field descriptor containing field name and its order in the class descriptor.
 *
 * @author @java.author
 * @version @java.version
 */
class GridToStringFieldDescriptor {
    /** Field name. */
    private final String name;

    /** */
    private int order = Integer.MAX_VALUE;

    /**
     * @param name Field name.
     */
    GridToStringFieldDescriptor(String name) {
        assert name != null;

        this.name = name;
    }

    /**
     * @return Field order.
     */
    int getOrder() { return order; }

    /**
     * @param order Field order.
     */
    void setOrder(int order) { this.order = order; }

    /**
     * @return Field name.
     */
    String getName() { return name; }
}
