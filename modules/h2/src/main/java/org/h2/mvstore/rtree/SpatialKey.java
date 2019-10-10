/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.mvstore.rtree;

import java.util.Arrays;

/**
 * A unique spatial key.
 */
public class SpatialKey {

    private final long id;
    private final float[] minMax;

    /**
     * Create a new key.
     *
     * @param id the id
     * @param minMax min x, max x, min y, max y, and so on
     */
    public SpatialKey(long id, float... minMax) {
        this.id = id;
        this.minMax = minMax;
    }

    /**
     * Get the minimum value for the given dimension.
     *
     * @param dim the dimension
     * @return the value
     */
    public float min(int dim) {
        return minMax[dim + dim];
    }

    /**
     * Set the minimum value for the given dimension.
     *
     * @param dim the dimension
     * @param x the value
     */
    public void setMin(int dim, float x) {
        minMax[dim + dim] = x;
    }

    /**
     * Get the maximum value for the given dimension.
     *
     * @param dim the dimension
     * @return the value
     */
    public float max(int dim) {
        return minMax[dim + dim + 1];
    }

    /**
     * Set the maximum value for the given dimension.
     *
     * @param dim the dimension
     * @param x the value
     */
    public void setMax(int dim, float x) {
        minMax[dim + dim + 1] = x;
    }

    public long getId() {
        return id;
    }

    public boolean isNull() {
        return minMax.length == 0;
    }

    @Override
    public String toString() {
        StringBuilder buff = new StringBuilder();
        buff.append(id).append(": (");
        for (int i = 0; i < minMax.length; i += 2) {
            if (i > 0) {
                buff.append(", ");
            }
            buff.append(minMax[i]).append('/').append(minMax[i + 1]);
        }
        return buff.append(")").toString();
    }

    @Override
    public int hashCode() {
        return (int) ((id >>> 32) ^ id);
    }

    @Override
    public boolean equals(Object other) {
        if (other == this) {
            return true;
        } else if (!(other instanceof SpatialKey)) {
            return false;
        }
        SpatialKey o = (SpatialKey) other;
        if (id != o.id) {
            return false;
        }
        return equalsIgnoringId(o);
    }

    /**
     * Check whether two objects are equals, but do not compare the id fields.
     *
     * @param o the other key
     * @return true if the contents are the same
     */
    public boolean equalsIgnoringId(SpatialKey o) {
        return Arrays.equals(minMax, o.minMax);
    }

}
