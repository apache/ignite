/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache;

import org.gridgain.grid.cache.query.*;

import java.io.*;

/**
 * Query test value.
 */
@SuppressWarnings("unused")
public class GridCacheQueryTestValue implements Serializable {
    /** */
    @GridCacheQueryTextField
    @GridCacheQuerySqlField(name = "fieldname")
    private String field1;

    /** */
    private int field2;

    /** */
    @GridCacheQuerySqlField(unique = true)
    private long field3;

    /** */
    @GridCacheQuerySqlField(orderedGroups = {
        @GridCacheQuerySqlField.Group(name = "grp1", order = 1),
        @GridCacheQuerySqlField.Group(name = "grp2", order = 2)})
    private long field4;

    /** */
    @GridCacheQuerySqlField(orderedGroups = {@GridCacheQuerySqlField.Group(name = "grp1", order = 2)})
    private long field5;

    /** */
    @GridCacheQuerySqlField(orderedGroups = {@GridCacheQuerySqlField.Group(name = "grp1", order = 3)})
    private GridCacheQueryEmbeddedValue field6 = new GridCacheQueryEmbeddedValue();

    /**
     *
     * @return Field.
     */
    public String getField1() {
        return field1;
    }

    /**
     *
     * @param field1 Field.
     */
    public void setField1(String field1) {
        this.field1 = field1;
    }

    /**
     *
     * @return Field.
     */
    @GridCacheQuerySqlField
    public int getField2() {
        return field2;
    }

    /**
     *
     * @param field2 Field.
     */
    public void setField2(int field2) {
        this.field2 = field2;
    }

    /**
     *
     * @return Field.
     */
    public long getField3() {
        return field3;
    }

    /**
     *
     * @param field3 Field.
     */
    public void setField3(long field3) {
        this.field3 = field3;
    }

    /**
     *
     * @return Field.
     */
    public long getField4() {
        return field4;
    }

    /**
     *
     * @param field4 Field.
     */
    public void setField4(long field4) {
        this.field4 = field4;
    }

    /**
     * @return Field.
     */
    public long getField5() {
        return field5;
    }

    /**
     * @param field5 Field.
     */
    public void setField5(long field5) {
        this.field5 = field5;
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"RedundantIfStatement"})
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        GridCacheQueryTestValue that = (GridCacheQueryTestValue)o;

        if (field2 != that.field2)
            return false;

        if (field3 != that.field3)
            return false;

        if (field1 != null ? !field1.equals(that.field1) : that.field1 != null)
            return false;

        return true;
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        int res = (field1 != null ? field1.hashCode() : 0);

        res = 31 * res + field2;
        res = 31 * res + (int)(field3 ^ (field3 >>> 32));

        return res;
    }

    /**
     * @param field6 Embedded value.
     */
    public void setField6(GridCacheQueryEmbeddedValue field6) {
        this.field6 = field6;
    }
}
