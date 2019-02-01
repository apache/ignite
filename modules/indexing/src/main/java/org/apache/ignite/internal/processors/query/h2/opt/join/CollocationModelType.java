/*
 * Copyright (c) 1997, 2014, Oracle and/or its affiliates. All rights reserved.
 * ORACLE PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 *
 *
 *
 *
 *
 *
 *
 *
 *
 *
 *
 *
 *
 *
 *
 *
 *
 *
 *
 *
 */

package org.apache.ignite.internal.processors.query.h2.opt.join;

/**
 * Collocation type.
 */
public enum CollocationModelType {
    /** */
    PARTITIONED_COLLOCATED(true, true),

    /** */
    PARTITIONED_NOT_COLLOCATED(true, false),

    /** */
    REPLICATED(false, true);

    /** */
    private final boolean partitioned;

    /** */
    private final boolean collocated;

    /**
     * @param partitioned Partitioned.
     * @param collocated Collocated.
     */
    CollocationModelType(boolean partitioned, boolean collocated) {
        this.partitioned = partitioned;
        this.collocated = collocated;
    }

    /**
     * @return {@code true} If partitioned.
     */
    public boolean isPartitioned() {
        return partitioned;
    }

    /**
     * @return {@code true} If collocated.
     */
    public boolean isCollocated() {
        return collocated;
    }

    /**
     * @param partitioned Partitioned.
     * @param collocated Collocated.
     * @return Type.
     */
    public static CollocationModelType of(boolean partitioned, boolean collocated) {
        if (collocated)
            return partitioned ? PARTITIONED_COLLOCATED : REPLICATED;

        assert partitioned;

        return PARTITIONED_NOT_COLLOCATED;
    }
}
