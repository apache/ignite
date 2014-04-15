/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.marshaller;

/**
 * Abstract class using in marshallers tests.
 */
public abstract class GridMarshallerTestAbstractBean {
    /** */
    private boolean flag = true;

    /**
     * @return flag value.
     */
    public boolean isFlag() {
        return flag;
    }

    /**
     * @param flag Flag value.
     */
    public void setFlag(boolean flag) {
        this.flag = flag;
    }
}
