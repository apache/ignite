/*
 * Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.value;

import org.h2.api.ErrorCode;
import org.h2.message.DbException;
import org.h2.util.geometry.EWKTUtils;

/**
 * Extended parameters of the GEOMETRY data type.
 */
public final class ExtTypeInfoGeometry extends ExtTypeInfo {

    private final int type;

    private final Integer srid;

    private static String toSQL(int type, Integer srid) {
        if (type == 0 && srid == null) {
            return "";
        }
        StringBuilder builder = new StringBuilder();
        builder.append('(');
        if (type == 0) {
            builder.append("GEOMETRY");
        } else {
            builder.append(EWKTUtils.formatGeometryTypeAndDimensionSystem(type));
        }
        if (srid != null) {
            builder.append(", ").append((int) srid);
        }
        return builder.append(')').toString();
    }

    /**
     * Creates new instance of extended parameters of the GEOMETRY data type.
     *
     * @param type
     *            the type and dimension system of geometries, or 0 if not
     *            constrained
     * @param srid
     *            the SRID of geometries, or {@code null} if not constrained
     */
    public ExtTypeInfoGeometry(int type, Integer srid) {
        this.type = type;
        this.srid = srid;
    }

    @Override
    public Value cast(Value value) {
        if (value.getValueType() != Value.GEOMETRY) {
            value = value.convertTo(Value.GEOMETRY);
        }
        ValueGeometry g = (ValueGeometry) value;
        if (type != 0 && g.getTypeAndDimensionSystem() != type || srid != null && g.getSRID() != srid) {
            throw DbException.get(ErrorCode.CHECK_CONSTRAINT_VIOLATED_1,
                    toSQL(g.getTypeAndDimensionSystem(), g.getSRID()) + " <> " + toString());
        }
        return g;
    }

    @Override
    public String getCreateSQL() {
        return toSQL(type, srid);
    }

}
