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
 * Checks that {@code @Override} annotations are located on the same line with target method declarartions.
 * </p>
 */
public class OverrideAnnotationOnTheSameLineCheck extends AbstractCheck {
    /** Different line error message. */
    public static final String DIFF_LINE_ERR_MSG =
        "@Override annotation on a different line than the target method declaration!";

    /** No method declaration error message. */
    public static final String NO_METHOD_ERR_MSG =
        "No method declaration atfer @Override annotation!";

    /** */
    public static final String OVERRIDE = "Override";

    /** {@inheritDoc} */
    @Override public int[] getDefaultTokens() {
        return new int[]{
            TokenTypes.METHOD_DEF,
        };
    }

    /** {@inheritDoc} */
    @Override public int[] getAcceptableTokens() {
        return CommonUtil.EMPTY_INT_ARRAY;
    }

    /** {@inheritDoc} */
    @Override public int[] getRequiredTokens() {
        return CommonUtil.EMPTY_INT_ARRAY;
    }

    /** {@inheritDoc} */
    @Override public void visitToken(DetailAST ast) {
        DetailAST nodeWithAnnotations = ast;

        if (ast.getType() == TokenTypes.TYPECAST)
            nodeWithAnnotations = ast.findFirstToken(TokenTypes.TYPE);

        DetailAST modifiersNode = nodeWithAnnotations.findFirstToken(TokenTypes.MODIFIERS);

        if (modifiersNode == null)
            modifiersNode = nodeWithAnnotations.findFirstToken(TokenTypes.ANNOTATIONS);

        if (modifiersNode != null) {
            for (DetailAST annotationNode = modifiersNode.getFirstChild();
                 annotationNode != null;
                 annotationNode = annotationNode.getNextSibling()) {
                if (annotationNode.getType() == TokenTypes.ANNOTATION
                    && OVERRIDE.equals(getAnnotationName(annotationNode))
                    && onDifferentLines(annotationNode))
                    log(annotationNode.getLineNo(), DIFF_LINE_ERR_MSG, getAnnotationName(annotationNode));
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

        if (nextNode == null)
            nextNode = node.getParent().getNextSibling();

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

        if (identNode == null)
            identNode = annotation.findFirstToken(TokenTypes.DOT).getLastChild();

        return identNode.getText();
    }

    /**
     * Checks whether or not annotation is located on a different line than the target method declaration.
     *
     * @param annotationNode {@code DetailAST} Annotation node.
     * @return {@code true} if annotation is located on a different line than the target method declaration or
     * {@code false} otherwise.
     */
    private boolean onDifferentLines(DetailAST annotationNode){
        DetailAST nextNode = getNextNode(annotationNode);

        if(nextNode == null)
            log(annotationNode.getLineNo(), NO_METHOD_ERR_MSG, getAnnotationName(annotationNode));

        return annotationNode.getLineNo() != nextNode.getLineNo();
    }
}
