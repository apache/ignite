/*
 * Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.util;

/**
 * Metadata of a column.
 *
 * <p>
 * Notice: {@linkplain #equals(Object)} and {@linkplain #hashCode()} use only
 * {@linkplain #name} field.
 * </p>
 */
public final class SimpleColumnInfo {
    /**
     * Name of the column.
     */
    public final String name;

    /**
     * Type of the column, see {@link java.sql.Types}.
     */
    public final int type;

    /**
     * Type name of the column.
     */
    public final String typeName;

    /**
     * Precision of the column
     */
    public final int precision;

    /**
     * Scale of the column.
     */
    public final int scale;

    /**
     * Creates metadata.
     *
     * @param name
     *            name of the column
     * @param type
     *            type of the column, see {@link java.sql.Types}
     * @param typeName
     *            type name of the column
     * @param precision
     *            precision of the column
     * @param scale
     *            scale of the column
     */
    public SimpleColumnInfo(String name, int type, String typeName, int precision, int scale) {
        this.name = name;
        this.type = type;
        this.typeName = typeName;
        this.precision = precision;
        this.scale = scale;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        SimpleColumnInfo other = (SimpleColumnInfo) obj;
        return name.equals(other.name);
    }

    @Override
    public int hashCode() {
        return name.hashCode();
    }
}
