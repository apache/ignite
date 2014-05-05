// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal;

/**
 * Component type.
 */
public enum GridComponentType {
    /** GGFS. */
    COMP_GGFS("org.gridgain.grid.kernal.processors.ggfs.GridGgfsOpProcessor"),

    /** Hadoop. */
    COMP_HADOOP("org.gridgain.grid.kernal.processors.hadoop.GridHadoopOpProcessor");

    /** Class name. */
    private final String clsName;

    /**
     * Constructor.
     *
     * @param clsName Class name.
     */
    private GridComponentType(String clsName) {
        this.clsName = clsName;
    }

    public String className() {
        return clsName;
    }
}
