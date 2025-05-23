package org.apache.ignite.internal.processors.query.calcite.exec.exp.window;

import java.util.List;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.ignite.internal.processors.query.calcite.type.IgniteTypeFactory;
import org.jetbrains.annotations.Nullable;

/** Interface for window function. */
interface WindowFunction<Row> {

    /**  */
    @Nullable Object call(Row row, int rowIdx, int peerIdx, WindowFunctionFrame<Row> frame);

    /**  */
    List<RelDataType> argumentTypes(IgniteTypeFactory typeFactory);

    /**  */
    RelDataType returnType(IgniteTypeFactory typeFactory);
}
