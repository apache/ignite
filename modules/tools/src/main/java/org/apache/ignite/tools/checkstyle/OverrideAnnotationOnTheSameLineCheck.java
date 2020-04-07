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

import com.puppycrawl.tools.checkstyle.api.AbstractCheck;
import com.puppycrawl.tools.checkstyle.api.DetailAST;
import com.puppycrawl.tools.checkstyle.api.TokenTypes;

import static com.puppycrawl.tools.checkstyle.api.TokenTypes.ANNOTATIONS;
import static com.puppycrawl.tools.checkstyle.api.TokenTypes.DOT;
import static com.puppycrawl.tools.checkstyle.api.TokenTypes.IDENT;
import static com.puppycrawl.tools.checkstyle.api.TokenTypes.METHOD_DEF;
import static com.puppycrawl.tools.checkstyle.api.TokenTypes.MODIFIERS;
import static com.puppycrawl.tools.checkstyle.utils.CommonUtil.EMPTY_INT_ARRAY;

/**
 * Checks that {@code @Override} annotations are located on the same line with target method declarartions.
 */
public class OverrideAnnotationOnTheSameLineCheck extends AbstractCheck {
    /** Different line error message. */
    private static final String DIFF_LINE_ERR_MSG =
        "@Override annotation on a different line than the target method declaration!";

    /** {@inheritDoc} */
    @Override public int[] getDefaultTokens() {
        return new int[] {
            METHOD_DEF
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

    /** {@inheritDoc} */
    @Override public void visitToken(DetailAST ast) {
        DetailAST node = ast.findFirstToken(MODIFIERS);

        if (node == null)
            node = ast.findFirstToken(ANNOTATIONS);

        if (node == null)
            return;

        for (node = node.getFirstChild(); node != null; node = node.getNextSibling()) {
            if (node.getType() == TokenTypes.ANNOTATION &&
                Override.class.getSimpleName().equals(annotationName(node)) &&
                node.getLineNo() != nextNode(node).getLineNo())
                log(node.getLineNo(), DIFF_LINE_ERR_MSG, annotationName(node));
        }
    }

    /**
     * Finds next node of ast tree.
     *
     * @param node {@code DetailAST} Current node.
     * @return {@code DetailAST} Node that is next to given.
     */
    private DetailAST nextNode(DetailAST node) {
        DetailAST nextNode = node.getNextSibling();

        return nextNode != null ? nextNode : node.getParent().getNextSibling();
    }

    /**
     * Returns the name of the given annotation.
     *
     * @param annotation Annotation node.
     * @return Annotation name.
     */
    private String annotationName(DetailAST annotation) {
        DetailAST identNode = annotation.findFirstToken(IDENT);

        if (identNode == null)
            identNode = annotation.findFirstToken(DOT).getLastChild();

        return identNode.getText();
    }
}
