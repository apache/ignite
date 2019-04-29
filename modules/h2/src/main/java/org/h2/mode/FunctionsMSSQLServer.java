/*
 * Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.mode;

import java.util.HashMap;

import org.h2.engine.Database;
import org.h2.expression.function.Function;
import org.h2.expression.function.FunctionInfo;

/**
 * Functions for {@link org.h2.engine.Mode.ModeEnum#MSSQLServer} compatibility
 * mode.
 */
public final class FunctionsMSSQLServer extends FunctionsBase {
    private static final HashMap<String, FunctionInfo> FUNCTIONS = new HashMap<>();

    static {
        copyFunction(FUNCTIONS, "LOCATE", "CHARINDEX");
        copyFunction(FUNCTIONS, "CURRENT_DATE", "GETDATE");
        copyFunction(FUNCTIONS, "LENGTH", "LEN");
        copyFunction(FUNCTIONS, "RANDOM_UUID", "NEWID");
    }

    /**
     * Returns mode-specific function for a given name, or {@code null}.
     *
     * @param database
     *            the database
     * @param upperName
     *            the upper-case name of a function
     * @return the function with specified name or {@code null}
     */
    public static Function getFunction(Database database, String upperName) {
        FunctionInfo info = FUNCTIONS.get(upperName);
        return info != null ? new Function(database, info) : null;
    }

    private FunctionsMSSQLServer(Database database, FunctionInfo info) {
        super(database, info);
    }
}
