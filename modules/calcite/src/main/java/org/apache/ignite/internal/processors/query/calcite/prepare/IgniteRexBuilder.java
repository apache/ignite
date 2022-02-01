package org.apache.ignite.internal.processors.query.calcite.prepare;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.sql.type.SqlTypeName;
import org.checkerframework.checker.nullness.qual.Nullable;

/** TODO */
public class IgniteRexBuilder extends RexBuilder {
    /**
     * Creates a RexBuilder.
     *
     * @param typeFactory Type factory
     */
    public IgniteRexBuilder(RelDataTypeFactory typeFactory) {
        super(typeFactory);
    }

    /** TODO */
    @Override protected RexLiteral makeLiteral(@Nullable Comparable o, RelDataType type, SqlTypeName typeName) {
        return super.makeLiteral(o, type, typeName);
    }
}
