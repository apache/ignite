package org.apache.ignite.internal.processors.query.calcite.util;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;

public class RexUtils {
    /** */
    public static RexNode makeCast(RexBuilder builder, RelDataType toType, RexNode node) {
        return TypeUtils.needCast(builder.getTypeFactory(), node.getType(), toType)
            ? builder.makeCast(toType, node)
            : node;
    }
}
