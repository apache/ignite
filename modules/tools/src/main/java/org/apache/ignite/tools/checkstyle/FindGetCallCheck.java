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

package org.apache.ignite.tools.checkstyle;

import java.util.HashMap;
import java.util.Map;
import com.puppycrawl.tools.checkstyle.api.AbstractCheck;
import com.puppycrawl.tools.checkstyle.api.DetailAST;

import static com.puppycrawl.tools.checkstyle.api.TokenTypes.DOT;
import static com.puppycrawl.tools.checkstyle.api.TokenTypes.ELIST;
import static com.puppycrawl.tools.checkstyle.api.TokenTypes.IDENT;
import static com.puppycrawl.tools.checkstyle.api.TokenTypes.METHOD_CALL;
import static com.puppycrawl.tools.checkstyle.api.TokenTypes.METHOD_DEF;
import static com.puppycrawl.tools.checkstyle.api.TokenTypes.TYPE;
import static com.puppycrawl.tools.checkstyle.api.TokenTypes.VARIABLE_DEF;
import static com.puppycrawl.tools.checkstyle.utils.CommonUtil.EMPTY_INT_ARRAY;

/**
 * Find all invocation of the {@code IgniteInternalFuture#get()} method.
 */
public class FindGetCallCheck extends AbstractCheck {
    /** */
    private static final Map<String, String> BLOCK_CALL_MAP = new HashMap<>();

    static {
        BLOCK_CALL_MAP.put("IgniteInternalFuture", "get");
        BLOCK_CALL_MAP.put("CountDownLatch", "await");
        BLOCK_CALL_MAP.put("CyclicBarrier", "await");
    }

    /** */
    private final Map<String, String> blockMethodVars = new HashMap<>();

    /** {@inheritDoc} */
    @Override public int[] getDefaultTokens() {
        return new int[] {
            VARIABLE_DEF, METHOD_CALL, METHOD_DEF
        };
    }

    /** {@inheritDoc} */
    @Override public int[] getAcceptableTokens() {
        return EMPTY_INT_ARRAY;
    }

    /** {@inheritDoc} */
    @Override public int[] getRequiredTokens() {
        return EMPTY_INT_ARRAY;
    }

    @Override public void visitToken(DetailAST ast) {
        if (ast.getType() == METHOD_DEF) {
            blockMethodVars.clear();
        } else if (ast.getType() == VARIABLE_DEF) {
            String type = variableType(ast);

            if (type == null)
                return;

            if (!BLOCK_CALL_MAP.containsKey(type))
                return;

            String varName = ast.findFirstToken(IDENT).getText();

            blockMethodVars.put(varName, type);
        }
        else if (ast.getType() == METHOD_CALL) {
            DetailAST dot = ast.findFirstToken(DOT);

            if (dot == null)
                return;

            DetailAST var = dot.findFirstToken(IDENT);

            if (var == null)
                return;

            DetailAST method = dot.getLastChild();

            boolean isBlockMethod = blockMethodVars.containsKey(var.getText()) &&
                BLOCK_CALL_MAP.containsValue(method.getText());

            if (!"wait".equals(method.getText()) && !isBlockMethod)
                return;

            DetailAST elist = ast.findFirstToken(ELIST);

            if (elist != null && elist.getChildCount() > 0)
                return;

            String type = blockMethodVars.get(var.getText());

            if (type == null)
                log(ast.getLineNo(), "Usage of the " + var.getText() + ".wait() prohibited");
            else
                log(ast.getLineNo(), "Usage of the " + type + "#" + method.getText() + "() prohibited");
        }
    }

    /** */
    private String variableType(DetailAST ast) {
        DetailAST type = ast.findFirstToken(TYPE);
        DetailAST ident = type.findFirstToken(IDENT);

        if (ident == null)
            return null;

        return ident.getText();
    }
}
