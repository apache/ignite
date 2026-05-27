package org.apache.ignite.internal.processors.query.calcite.sql.fun;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.calcite.sql.fun.SqlLeadLagAggFunction;
import org.apache.calcite.sql.type.SqlTypeTransforms;

/** {@link SqlLeadLagAggFunction}, with enforced return type nullability. */
public class SqlLeadLagFunction extends SqlLeadLagAggFunction {
    /** */
    SqlLeadLagFunction(SqlKind kind) {
        super(kind);
    }

    /** {@inheritDoc} */
    @Override public RelDataType inferReturnType(SqlOperatorBinding opBinding) {
        return SqlTypeTransforms.FORCE_NULLABLE.transformType(opBinding, super.inferReturnType(opBinding));
    }
}
