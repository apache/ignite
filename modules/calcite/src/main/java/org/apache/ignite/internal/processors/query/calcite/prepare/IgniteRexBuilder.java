package org.apache.ignite.internal.processors.query.calcite.prepare;

import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;

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

//    @Override public RexLiteral makeCharLiteral(NlsString str) {
//        RexLiteral rexLiteral = super.makeCharLiteral(str);
//
//
//    }
}
