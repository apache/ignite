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
import com.puppycrawl.tools.checkstyle.utils.CommonUtil;

/**
 * <p>
 * Checks that {@code @Override} annotations are located on the same line with method declarartions.
 * </p>
 */
public class OverrideAnnotationOnTheSameLineCheck extends AbstractCheck {
    /** Error message. */
    public static final String ERR_MSG = "@Override annotation on different line than method declaration.";

    /** */
    public static final String OVERRIDE = "Override";

    /** {@inheritDoc} */
    @Override public int[] getDefaultTokens() {
        return new int[]{
            TokenTypes.CLASS_DEF,
            TokenTypes.INTERFACE_DEF,
            TokenTypes.ENUM_DEF,
            TokenTypes.METHOD_DEF,
            TokenTypes.CTOR_DEF,
        };
    }

    /** {@inheritDoc} */
    @Override public int[] getAcceptableTokens() {
        return new int[]{
            TokenTypes.CLASS_DEF,
            TokenTypes.INTERFACE_DEF,
            TokenTypes.ENUM_DEF,
            TokenTypes.METHOD_DEF,
            TokenTypes.ANNOTATION_DEF,
        };
    }

    /** {@inheritDoc} */
    @Override public int[] getRequiredTokens() {
        return CommonUtil.EMPTY_INT_ARRAY;
    }

    /** {@inheritDoc} */
    @Override public void visitToken(DetailAST ast) {
        DetailAST nodeWithAnnotations = ast;

        if (ast.getType() == TokenTypes.TYPECAST) {
            nodeWithAnnotations = ast.findFirstToken(TokenTypes.TYPE);
        }

        DetailAST modifiersNode = nodeWithAnnotations.findFirstToken(TokenTypes.MODIFIERS);

        if (modifiersNode == null) {
            modifiersNode = nodeWithAnnotations.findFirstToken(TokenTypes.ANNOTATIONS);
        }

        if (modifiersNode != null) {
            for (DetailAST annotationNode = modifiersNode.getFirstChild();
                 annotationNode != null;
                 annotationNode = annotationNode.getNextSibling()) {
                if (annotationNode.getType() == TokenTypes.ANNOTATION
                    && OVERRIDE.equals(getAnnotationName(annotationNode))
                    && annotationNode.getLineNo() != getNextNode(annotationNode).getLineNo()) {
                    log(annotationNode.getLineNo(), ERR_MSG, getAnnotationName(annotationNode));
                }
            }
        }
    }

    /**
     * Finds next node of ast tree.
     *
     * @param node {@code DetailAST} Current node
     * @return {@code DetailAST} Node that is next to given.
     */
    private static DetailAST getNextNode(DetailAST node) {
        DetailAST nextNode = node.getNextSibling();

        if (nextNode == null) {
            nextNode = node.getParent().getNextSibling();
        }

        return nextNode;
    }

    /**
     * Returns the name of the given annotation.
     *
     * @param annotation {@code DetailAST} Annotation node.
     * @return {@code String} Annotation name.
     */
    private static String getAnnotationName(DetailAST annotation) {
        DetailAST identNode = annotation.findFirstToken(TokenTypes.IDENT);

        if (identNode == null) {
            identNode = annotation.findFirstToken(TokenTypes.DOT).getLastChild();
        }

        return identNode.getText();
    }
}
