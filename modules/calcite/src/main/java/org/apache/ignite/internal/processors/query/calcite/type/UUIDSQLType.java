/** TODO */
package org.apache.ignite.internal.processors.query.calcite.type;

import org.apache.calcite.rel.type.RelDataTypeFamily;
import org.apache.calcite.rel.type.RelDataTypeImpl;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;

/** TODO */
public class UUIDSQLType extends RelDataTypeImpl {
    /** TODO */
    UUIDSQLType() {
        computeDigest();
    }

    /** TODO */
    @Override public SqlTypeName getSqlTypeName() {
        return SqlTypeName.ANY;
    }

    /** TODO */
    @Override public RelDataTypeFamily getFamily() {
        return SqlTypeFamily.ANY;
    }

    /** TODO */
    @Override protected void generateTypeString(StringBuilder sb, boolean withDetail) {
        sb.append("UUID");
    }
}
