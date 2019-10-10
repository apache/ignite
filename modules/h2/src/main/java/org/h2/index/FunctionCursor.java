/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.index;

import org.h2.engine.Session;
import org.h2.result.ResultInterface;
import org.h2.result.SearchRow;

/**
 * A cursor for a function that returns a result.
 */
public class FunctionCursor extends AbstractFunctionCursor {

    private final ResultInterface result;

    FunctionCursor(FunctionIndex index, SearchRow first, SearchRow last, Session session, ResultInterface result) {
        super(index, first, last, session);
        this.result = result;
    }

    @Override
    boolean nextImpl() {
        row = null;
        if (result != null && result.next()) {
            values = result.currentRow();
        } else {
            values = null;
        }
        return values != null;
    }

}
