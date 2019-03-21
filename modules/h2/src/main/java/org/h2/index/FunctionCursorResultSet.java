/*
 * Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.index;

import org.h2.engine.Session;
import org.h2.result.ResultInterface;
import org.h2.result.SearchRow;
import org.h2.value.Value;

import java.util.Arrays;

/**
 * A cursor for a function that returns a JDBC result set.
 */
public class FunctionCursorResultSet extends AbstractFunctionCursor {

    private final ResultInterface result;

    FunctionCursorResultSet(FunctionIndex index, SearchRow first, SearchRow last, Session session,
            ResultInterface result) {
        super(index, first, last, session);
        this.result = result;
    }

    @Override
    boolean nextImpl() {
        row = null;
        if (result != null && result.next()) {
            int columnCount = result.getVisibleColumnCount();
            Value[] currentRow = result.currentRow();
            values = Arrays.copyOf(currentRow, columnCount);
        } else {
            values = null;
        }
        return values != null;
    }

}