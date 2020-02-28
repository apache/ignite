package org.apache.ignite.internal.processors.service.inner.experimental;

import java.io.Serializable;

/**
 * Exhibits same name as {@link org.apache.ignite.internal.processors.service.inner.impl.Param} but with different
 * package.
 */
public class Param implements Serializable {
    /** */
    private static final long serialVersionUID =0L;

    /** Just a value. */
    public static final int VALUE = 100;

    /**
     * @return Some known value.
     */
    public int value() {
        return VALUE;
    }
}
