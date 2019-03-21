/*
 * Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.expression.aggregate;

import java.util.ArrayList;

import org.h2.engine.Database;
import org.h2.expression.Expression;
import org.h2.expression.ExpressionColumn;
import org.h2.index.Index;
import org.h2.mvstore.db.MVSpatialIndex;
import org.h2.table.Column;
import org.h2.table.TableFilter;
import org.h2.util.geometry.GeometryUtils;
import org.h2.value.Value;
import org.h2.value.ValueGeometry;
import org.h2.value.ValueNull;

/**
 * Data stored while calculating an aggregate.
 */
class AggregateDataEnvelope extends AggregateData {

    private double[] envelope;

    /**
     * Get the index (if any) for the column specified in the geometry
     * aggregate.
     *
     * @param on
     *            the expression (usually a column expression)
     * @return the index, or null
     */
    static Index getGeometryColumnIndex(Expression on) {
        if (on instanceof ExpressionColumn) {
            ExpressionColumn col = (ExpressionColumn) on;
            Column column = col.getColumn();
            if (column.getType().getValueType() == Value.GEOMETRY) {
                TableFilter filter = col.getTableFilter();
                if (filter != null) {
                    ArrayList<Index> indexes = filter.getTable().getIndexes();
                    if (indexes != null) {
                        for (int i = 1, size = indexes.size(); i < size; i++) {
                            Index index = indexes.get(i);
                            if (index instanceof MVSpatialIndex && index.isFirstColumn(column)) {
                                return index;
                            }
                        }
                    }
                }
            }
        }
        return null;
    }

    @Override
    void add(Database database, Value v) {
        if (v == ValueNull.INSTANCE) {
            return;
        }
        envelope = GeometryUtils.union(envelope, ((ValueGeometry) v.convertTo(Value.GEOMETRY)).getEnvelopeNoCopy());
    }

    @Override
    Value getValue(Database database, int dataType) {
        return ValueGeometry.fromEnvelope(envelope);
    }

}
