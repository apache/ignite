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
 * Base class for mode-specific functions.
 */
abstract class FunctionsBase extends Function {

    FunctionsBase(Database database, FunctionInfo info) {
        super(database, info);
    }

    /**
     * Copy a standard function to a mode functions with a different name.
     *
     * @param functions
     *            mode functions
     * @param stdName
     *            the name of the standard function
     * @param newName
     *            the name of the mode-specific function
     */
    static void copyFunction(HashMap<String, FunctionInfo> functions, String stdName, String newName) {
        functions.put(newName, new FunctionInfo(Function.getFunctionInfo(stdName), newName));
    }

}
