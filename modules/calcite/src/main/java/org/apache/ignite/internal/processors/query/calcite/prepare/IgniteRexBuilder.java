package org.apache.ignite.internal.processors.query.calcite.prepare;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.util.NlsString;

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
