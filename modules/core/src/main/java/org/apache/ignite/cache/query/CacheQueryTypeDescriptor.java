/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.cache.query;

import org.apache.ignite.internal.util.typedef.internal.*;

/**
 * Type descriptor for field in java and database.
 */
public class CacheQueryTypeDescriptor {
    /** Column name in database. */
    private String javaName;

    /** Corresponding java type. */
    private Class<?> javaType;

    /** Column name in database. */
    private String dbName;

    /** Column JDBC type in database. */
    private int dbType;

    /**
     * Default constructor.
     */
    public CacheQueryTypeDescriptor() {
        // No-op.
    }

    /**
     * @param javaName Field name in java object.
     * @param javaType Field java type.
     * @param dbName Column name in database.
     * @param dbType Column JDBC type in database.
     */
    public CacheQueryTypeDescriptor(String javaName, Class<?> javaType, String dbName, int dbType) {
        this.javaName = javaName;
        this.javaType = javaType;
        this.dbName = dbName;
        this.dbType = dbType;
    }

    /**
     * @return Field name in java object.
     */
    public String getJavaName() {
        return javaName;
    }

    /**
     * @param javaName Field name in java object.
     */
    public void setJavaName(String javaName) {
        this.javaName = javaName;
    }

    /**
     * @return Field java type.
     */
    public Class<?> getJavaType() {
        return javaType;
    }

    /**
     * @param javaType Corresponding java type.
     */
    public void setJavaType(Class<?> javaType) {
        this.javaType = javaType;
    }

    /**
     * @return Column name in database.
     */
    public String getDbName() {
        return dbName;
    }

    /**
     * @param dbName Column name in database.
     */
    public void setDbName(String dbName) {
        this.dbName = dbName;
    }

    /**
     * @return Column JDBC type in database.
     */
    public int getDbType() {
        return dbType;
    }

    /**
     * @param dbType Column JDBC type in database.
     */
    public void setDbType(int dbType) {
        this.dbType = dbType;
    }

     /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (!(o instanceof CacheQueryTypeDescriptor))
            return false;

        CacheQueryTypeDescriptor that = (CacheQueryTypeDescriptor)o;

        return javaName.equals(that.javaName) && dbName.equals(that.dbName) &&
            javaType == that.javaType && dbType == that.dbType;
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        int res = javaName.hashCode();

        res = 31 * res + dbName.hashCode();
        res = 31 * res + javaType.hashCode();
        res = 31 * res + dbType;

        return res;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(CacheQueryTypeDescriptor.class, this);
    }
}
