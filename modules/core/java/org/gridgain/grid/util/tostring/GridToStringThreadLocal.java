// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.util.tostring;

import org.gridgain.grid.util.typedef.internal.*;

/**
 * Helper wrapper containing StringBuilder and additional values. Stored as a thread-lcal variable.
 *
 * @author @java.author
 * @version @java.version
 */
class GridToStringThreadLocal {
    /** */
    private SB sb = new SB(256);

    /** */
    private Object[] addNames = new Object[5];

    /** */
    private Object[] addVals = new Object[5];

    /**
     * @return String builder.
     */
    SB getStringBuilder() {
        return sb;
    }

    /**
     * @return Additional names.
     */
    Object[] getAdditionalNames() {
        return addNames;
    }

    /**
     * @return Additional values.
     */
    Object[] getAdditionalValues() {
        return addVals;
    }
}
