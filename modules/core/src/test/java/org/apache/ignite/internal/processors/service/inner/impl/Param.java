package org.apache.ignite.internal.processors.service.inner.impl;

import java.io.Serializable;

/**
 * Exhibits same name as {@link org.apache.ignite.internal.processors.service.inner.experimental.Param} but with
 * different package.
 */
public class Param implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Just a value. */
    public static final int VALUE = 17;

    /**
     * @return Some known value.
     */
    public int value() {
        return VALUE;
    }
}
