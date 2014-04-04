/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.session;

/**
 * Thread serial number.
 */
class GridThreadSerialNumber {
    /** The next serial number to be assigned. */
    private int nextSerialNum = 0;

    /** */
    private ThreadLocal<Integer> serialNum = new ThreadLocal<Integer>() {
        @Override protected synchronized Integer initialValue() {
            return nextSerialNum++;
        }
    };

    /**
     * @return Serial number value.
     */
    public int get() {
        return serialNum.get();
    }
}
