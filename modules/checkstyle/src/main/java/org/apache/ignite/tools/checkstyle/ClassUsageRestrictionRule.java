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
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import com.puppycrawl.tools.checkstyle.api.AbstractCheck;
import com.puppycrawl.tools.checkstyle.api.DetailAST;

import static com.puppycrawl.tools.checkstyle.api.FullIdent.createFullIdent;
import static com.puppycrawl.tools.checkstyle.api.TokenTypes.EXTENDS_CLAUSE;
import static com.puppycrawl.tools.checkstyle.api.TokenTypes.IMPORT;
import static com.puppycrawl.tools.checkstyle.api.TokenTypes.LITERAL_NEW;
import static com.puppycrawl.tools.checkstyle.api.TokenTypes.METHOD_CALL;
import static com.puppycrawl.tools.checkstyle.api.TokenTypes.METHOD_REF;
import static com.puppycrawl.tools.checkstyle.api.TokenTypes.STATIC_IMPORT;

/** */
public class ClassUsageRestrictionRule extends AbstractCheck {
    /** */
    private static final int[] TOKENS = new int[] {
        IMPORT,
        STATIC_IMPORT,
        EXTENDS_CLAUSE,
        LITERAL_NEW,
        METHOD_REF,
        METHOD_CALL
    };

    /** */
    private static final Set<String> DEFAULT_RESTRICTED_FACTORY_METHODS = Set.of("new");

    /** */
    private ClassDescriptor restrictedCls;

    /** */
    private Set<String> restrictedFactoryMethods = DEFAULT_RESTRICTED_FACTORY_METHODS;

    /** */
    private String substitutionClsName;

    /** */
    private boolean isRestrictedClsImportDetected;

    /** */
    private final Set<String> detectedMethodImports = new HashSet<>();

    /** {@inheritDoc} */
    @Override public void init() {
        if (restrictedCls == null)
            throw new IllegalStateException("Restricted class is not set");

        if (isEmpty(substitutionClsName))
            throw new IllegalStateException("Substitution class is not set");

        if (isEmpty(restrictedCls.packageName) || isEmpty(restrictedCls.simpleName))
            throw new IllegalStateException("Invalid restricted class name. Class FQN is required");
    }

    /** {@inheritDoc} */
    @Override public void beginTree(DetailAST rootAST) {
        isRestrictedClsImportDetected = "java.lang".equals(restrictedCls.packageName);

        detectedMethodImports.clear();
    }

    /** {@inheritDoc} */
    @Override public void visitToken(DetailAST token) {
        if (token.getType() == IMPORT) {
            ClassDescriptor clsDesc = ClassDescriptor.forToken(token.getFirstChild());

            if (isRestrictedClass(clsDesc))
                isRestrictedClsImportDetected = true;
        }
        else if (token.getType() == STATIC_IMPORT) {
            ClassMemberDescriptor memberDesc = ClassMemberDescriptor.fromToken(token.getFirstChild().getNextSibling());

            if (isRestrictedMethod(memberDesc))
                detectedMethodImports.add(memberDesc.memberName);
        }
        else if (token.getType() == EXTENDS_CLAUSE || token.getType() == LITERAL_NEW) {
            ClassDescriptor clsDesc = ClassDescriptor.forToken(token.getFirstChild());

            if (isRestrictedClass(clsDesc))
                logFailure(token);
        }
        else if (token.getType() == METHOD_REF) {
            ClassMemberDescriptor methodDesc = ClassMemberDescriptor.fromMethodReferenceToken(token);

            if (isRestrictedMethod(methodDesc))
                logFailure(token);
        }
        else if (token.getType() == METHOD_CALL) {
            ClassMemberDescriptor methodDesc = ClassMemberDescriptor.fromToken(token.getFirstChild());

            if (isRestrictedMethod(methodDesc))
                logFailure(token);
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
        Set<String> restrictedFactoryMethods = new HashSet<>(DEFAULT_RESTRICTED_FACTORY_METHODS);

        Arrays.stream(factoryMethods.split(",")).map(String::trim).forEach(restrictedFactoryMethods::add);

        this.restrictedFactoryMethods = restrictedFactoryMethods;
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
    private boolean isRestrictedMethod(ClassMemberDescriptor desc) {
        if (desc.clsDesc == null)
            return detectedMethodImports.contains(desc.memberName);

        return restrictedFactoryMethods.contains(desc.memberName) && isRestrictedClass(desc.clsDesc);
    }

    /** */
    private boolean isRestrictedClass(ClassDescriptor desc) {
        if (Objects.equals(restrictedCls.fullName, desc.fullName))
            return true;

        return Objects.equals(restrictedCls.simpleName, desc.simpleName) && isRestrictedClsImportDetected;
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
            this.fullName = fullName;
            this.packageName = packageName;
            this.simpleName = simpleName;
        }

        /** */
        public static ClassDescriptor forToken(DetailAST token) {
            return forName(createFullIdent(token).getText());
        }

        /** */
        public static ClassDescriptor forName(String name) {
            int sepPos = name.lastIndexOf('.');

            if (sepPos == -1)
                return new ClassDescriptor(name, "", name);

            return new ClassDescriptor(name, name.substring(0, sepPos), name.substring(sepPos + 1));
        }
    }

    /** */
    private static class ClassMemberDescriptor {
        /** */
        private final ClassDescriptor clsDesc;

        /** */
        private final String memberName;

        /** */
        private ClassMemberDescriptor(ClassDescriptor clsDesc, String memberName) {
            this.clsDesc = clsDesc;
            this.memberName = memberName;
        }

        /** */
        public static ClassMemberDescriptor fromMethodReferenceToken(DetailAST token) {
            DetailAST clsToken = token.getFirstChild();

            DetailAST methodToken = clsToken.getNextSibling();

            return new ClassMemberDescriptor(ClassDescriptor.forToken(clsToken), methodToken.getText());
        }

        /** */
        public static ClassMemberDescriptor fromToken(DetailAST token) {
            final String tokenText = createFullIdent(token).getText();

            final int memberSepPos = tokenText.lastIndexOf('.');

            if (memberSepPos == -1)
                return new ClassMemberDescriptor(null, tokenText);

            return new ClassMemberDescriptor(
                ClassDescriptor.forName(tokenText.substring(0, memberSepPos)),
                tokenText.substring(memberSepPos + 1)
            );
        }
    }
}
