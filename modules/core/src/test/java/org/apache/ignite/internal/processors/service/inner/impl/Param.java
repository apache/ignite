package org.apache.ignite.internal.processors.service.inner.impl;

import java.io.Serializable;

/**
 * Exhibits same name as {@link org.apache.ignite.internal.processors.service.inner.experimental.Param} but different
 * package.
 */
public class Param implements Serializable {
    /** */
    private static final long serialVersionUID = 1L;

    /** */
    public static final int VALUE = 17;

    /** */
    public int value(){ return VALUE; }
}
