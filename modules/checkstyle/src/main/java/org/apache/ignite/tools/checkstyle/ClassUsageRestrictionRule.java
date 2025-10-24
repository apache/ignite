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

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import com.puppycrawl.tools.checkstyle.api.AbstractCheck;
import com.puppycrawl.tools.checkstyle.api.DetailAST;

import static com.puppycrawl.tools.checkstyle.api.FullIdent.createFullIdent;
import static com.puppycrawl.tools.checkstyle.api.FullIdent.createFullIdentBelow;
import static com.puppycrawl.tools.checkstyle.api.TokenTypes.EXTENDS_CLAUSE;
import static com.puppycrawl.tools.checkstyle.api.TokenTypes.IMPORT;
import static com.puppycrawl.tools.checkstyle.api.TokenTypes.LITERAL_NEW;
import static com.puppycrawl.tools.checkstyle.api.TokenTypes.METHOD_CALL;
import static com.puppycrawl.tools.checkstyle.api.TokenTypes.METHOD_REF;
import static com.puppycrawl.tools.checkstyle.api.TokenTypes.STATIC_IMPORT;

/** */
public class ClassUsageRestrictionRule extends AbstractCheck {
    /** */
    private ClassDescriptor restrictedCls;

    /** */
    private Set<String> restrictedFactoryMethods = Collections.emptySet();

    /** */
    private String substitutionClsName;

    /** */
    private boolean isRestrictedClsImportDetected;

    /** */
    private final Set<String> restrictedStaticMethodImports = new HashSet<>();

    /** */
    private static final int[] TOKENS = new int[] {
        IMPORT,
        STATIC_IMPORT,
        EXTENDS_CLAUSE,
        LITERAL_NEW,
        METHOD_REF,
        METHOD_CALL
    };

    @Override public void init() {
        if (restrictedCls == null)
            throw new IllegalStateException("Restricted class is not set");

        if (isEmpty(substitutionClsName))
            throw new IllegalStateException("Substitution class is not set");
    }

    /** {@inheritDoc} */
    @Override public void beginTree(DetailAST rootAST) {
        isRestrictedClsImportDetected = false;

        restrictedStaticMethodImports.clear();
    }

    /** {@inheritDoc} */
    @Override public void visitToken(DetailAST token) {
        if (token.getType() == IMPORT) {
            String clsImport = createFullIdentBelow(token).getText();

            if (Objects.equals(restrictedCls.fullName, clsImport))
                isRestrictedClsImportDetected = true;
        }
        else if (token.getType() == STATIC_IMPORT) {
            ClassMemberDescriptor desc = ClassMemberDescriptor.fromToken(token.getFirstChild().getNextSibling());

            if (restrictedCls.fullName.equals(desc.clsName))
                restrictedStaticMethodImports.add(desc.memberName);
        }
        else if (token.getType() == EXTENDS_CLAUSE || token.getType() == LITERAL_NEW) {
            if (isRestrictedClass(createFullIdentBelow(token).getText()))
                logFailure(token);
        }
        else if (token.getType() == METHOD_REF) {
            DetailAST clsToken = token.getFirstChild();

            DetailAST methodToken = clsToken.getNextSibling();

            if (restrictedFactoryMethods.contains(methodToken.getText()) && isRestrictedClass(createFullIdent(clsToken).getText()))
                logFailure(token);
        }
        else if (token.getType() == METHOD_CALL) {
            ClassMemberDescriptor desc = ClassMemberDescriptor.fromToken(token.getFirstChild());

            if (
                restrictedFactoryMethods.contains(desc.memberName) &&
                    (restrictedStaticMethodImports.contains(desc.memberName) || isRestrictedClass(desc.clsName))
            ) {
                logFailure(token);
            }
        }
        else
            throw new IllegalStateException();
    }

    /** {@inheritDoc} */
    @Override public int[] getDefaultTokens() {
        return TOKENS.clone();
    }

    /** {@inheritDoc} */
    @Override public int[] getAcceptableTokens() {
        return TOKENS.clone();
    }

    /** {@inheritDoc} */
    @Override public int[] getRequiredTokens() {
        return TOKENS.clone();
    }

    /** */
    public void setClassName(String clsName) {
        restrictedCls = ClassDescriptor.forName(clsName);
    }

    /** */
    public void setFactoryMethods(String factoryMethods) {
        restrictedFactoryMethods = Arrays.stream(factoryMethods.split(",")).map(String::trim).collect(Collectors.toSet());
    }

    /** */
    public void setSubstitutionClassName(String substitutionClsName) {
        this.substitutionClsName = substitutionClsName;
    }

    /** */
    private void logFailure(DetailAST token) {
        log(
            token,
            "Usage of {0} class and its factory methods is restricted. Use {1} class as a substitution",
            restrictedCls.fullName,
            substitutionClsName
        );
    }

    /** */
    private static boolean isEmpty(String str) {
        return str == null || str.isEmpty();
    }

    /** */
    private boolean isRestrictedClass(String clsName) {
        if (Objects.equals(restrictedCls.fullName, clsName))
            return true;

        return Objects.equals(restrictedCls.simpleName, clsName) &&
            (isRestrictedClsImportDetected || "java.lang".equals(restrictedCls.packageName));
    }

    /** */
    private static class ClassDescriptor {
        /** */
        private final String fullName;

        /** */
        private final String packageName;

        /** */
        private final String simpleName;

        /** */
        public ClassDescriptor(String fullName, String packageName, String simpleName) {
            if (isEmpty(fullName) || isEmpty(packageName) || isEmpty(simpleName))
                throw new IllegalArgumentException("Invalid class name");

            this.fullName = fullName;
            this.packageName = packageName;
            this.simpleName = simpleName;
        }

        /** */
        public static ClassDescriptor forName(String clsName) {
            int sepPos = clsName.lastIndexOf('.');

            if (sepPos == -1)
                throw new IllegalArgumentException("Invalid class name");

            return new ClassDescriptor(clsName, clsName.substring(0, sepPos), clsName.substring(sepPos + 1));
        }
    }

    /** */
    private static class ClassMemberDescriptor {
        /** */
        private final String clsName;

        /** */
        private final String memberName;

        /** */
        public ClassMemberDescriptor(String clsName, String memberName) {
            this.clsName = clsName;
            this.memberName = memberName;
        }

        /** */
        public static ClassMemberDescriptor fromToken(DetailAST token) {
            final String text = createFullIdent(token).getText();

            final int memberSepPos = text.lastIndexOf('.');

            if (memberSepPos == -1)
                return new ClassMemberDescriptor("", text);

            return new ClassMemberDescriptor(
                text.substring(0, memberSepPos),
                text.substring(memberSepPos + 1)
            );
        }
    }
}
