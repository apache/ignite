/** TODO */
package org.apache.ignite.internal.processors.query.calcite.type;

import org.apache.calcite.rel.type.RelDataTypeFamily;
import org.apache.calcite.rel.type.RelDataTypeImpl;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;

/** UUID SQL type. */
public class UUIDType extends RelDataTypeImpl {
    /** Nullable flag. */
    private final boolean nullable;

    /** Ctor. */
    public UUIDType(boolean nullable) {
        this.nullable = nullable;

        computeDigest();
    }

    /** {@inheritDoc} */
    @Override public boolean isNullable() {
        return nullable;
    }

    /** {@inheritDoc} */
    @Override public RelDataTypeFamily getFamily() {
        return SqlTypeFamily.ANY;
    }

    /** {@inheritDoc} */
    @Override public SqlTypeName getSqlTypeName() {
        return SqlTypeName.ANY;
    }

    /** {@inheritDoc} */
    @Override protected void generateTypeString(StringBuilder sb, boolean withDetail) {
        sb.append("UUID");
    }
}
