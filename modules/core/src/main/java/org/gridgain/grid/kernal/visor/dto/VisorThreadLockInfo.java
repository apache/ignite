/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.visor.dto;

import java.io.*;
import java.lang.management.*;

/**
 * Data transfer object for {@link LockInfo}.
 */
public class VisorThreadLockInfo implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /**
     * Fully qualified name of the class of the lock object.
     */
    protected final String className;

    /**
     * Identity hash code of the lock object.
     */
    protected final Integer identityHashCode;

    /** Create thread lock info with given parameters. */
    public VisorThreadLockInfo(String className, Integer identityHashCode) {
        assert className != null;

        this.className = className;
        this.identityHashCode = identityHashCode;
    }

    /** Create data transfer object for given lock info. */
    public static VisorThreadLockInfo from(LockInfo li) {
        assert li != null;

        return new VisorThreadLockInfo(li.getClassName(), li.getIdentityHashCode());
    }

    /**
     * @return Fully qualified name of the class of the lock object.
     */
    public String className() {
        return className;
    }

    /**
     * @return Identity hash code of the lock object.
     */
    public Integer identityHashCode() {
        return identityHashCode;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return className + '@' + Integer.toHexString(identityHashCode);
    }
}
