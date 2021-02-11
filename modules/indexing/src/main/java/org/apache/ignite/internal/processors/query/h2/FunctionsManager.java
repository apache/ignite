/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.query.h2;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import org.h2.expression.Function;

/**
 * SQL function manager.
 */
@SuppressWarnings("unchecked")
public class FunctionsManager {
    /** Original H2 functions set. */
    private static HashMap<String, Object> origFuncs;

    /** Current H2 functions set. */
    private static HashMap<String, Object> funcs;

    static {
        // Extract from H2 the set of available functions.
        try {
            Field fldFUNCTIONS = Function.class.getDeclaredField("FUNCTIONS");

            fldFUNCTIONS.setAccessible(true);

            funcs = (HashMap<String, Object>)fldFUNCTIONS.get(Class.class);

            origFuncs = new HashMap<>(funcs);
        }
        catch (NoSuchFieldException | IllegalAccessException e) {
            e.printStackTrace();
        }
    }

    /**
     *
     */
    public FunctionsManager(DistributedSqlConfiguration distSqlCfg) {
        assert Objects.nonNull(funcs);
        assert Objects.nonNull(origFuncs);
        distSqlCfg.listenDisabledFunctions(this::updateDisabledFunctions);
    }

    /**
     * Listener of changes the SQL parameter 'disabled functions'.
     *
     * @param paramName Parameter name (unused)
     * @param oldDisabledFuncs Old set of disabled functions.
     * @param newDisabledFuncs New set of disabled functions.
     */
    private void updateDisabledFunctions(
        String paramName,
        HashSet<String> oldDisabledFuncs,
        HashSet<String> newDisabledFuncs) {
        if (newDisabledFuncs != null)
            removeFunctions(newDisabledFuncs);
        else
            removeFunctions(DistributedSqlConfiguration.DFLT_DISABLED_FUNCS);
    }

    /**
     * @param funcNames Set of function that must be removed from original functions set.
     */
    private static void removeFunctions(Set<String> funcNames) {
        funcs.putAll(origFuncs);

        funcs.keySet().removeAll(funcNames);
    }
}
