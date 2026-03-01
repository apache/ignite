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

import java.io.File;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import com.puppycrawl.tools.checkstyle.Checker;
import com.puppycrawl.tools.checkstyle.DefaultConfiguration;
import com.puppycrawl.tools.checkstyle.api.AuditEvent;
import com.puppycrawl.tools.checkstyle.api.AuditListener;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/** */
public class ClassUsageRestrictionRuleTest {
    /** */
    private static final String RESTRICTED_CLASS = "java.util.concurrent.ForkJoinPool";

    /** */
    private static final String[] RESTRICTED_FACTORY_METHODS = new String[] {
        "commonPool"
    };

    /** */
    private static final String SUBSTITUTION_CLASS = "org.apache.ignite.AllowedClass";

    /** */
    @Test
    public void testExtends() throws Exception {
        assertCheckstyleFailure("RestrictedExtends.java", RESTRICTED_CLASS);
    }

    /** */
    @Test
    public void testExtendsFullName() throws Exception {
        assertCheckstyleFailure("RestrictedExtendsFullName.java", RESTRICTED_CLASS);
    }

    /** */
    @Test
    public void testFactoryMethod() throws Exception {
        assertCheckstyleFailure("RestrictedFactoryMethod.java", RESTRICTED_CLASS, RESTRICTED_FACTORY_METHODS);
    }

    /** */
    @Test
    public void testFactoryMethodFullName() throws Exception {
        assertCheckstyleFailure("RestrictedFactoryMethodFullName.java", RESTRICTED_CLASS, RESTRICTED_FACTORY_METHODS);
    }

    /** */
    @Test
    public void testFactoryMethodStaticImport() throws Exception {
        assertCheckstyleFailure("RestrictedFactoryMethodStaticImport.java", RESTRICTED_CLASS, RESTRICTED_FACTORY_METHODS);
    }

    /** */
    @Test
    public void testFactoryMethodReference() throws Exception {
        assertCheckstyleFailure("RestrictedFactoryMethodReference.java", RESTRICTED_CLASS, RESTRICTED_FACTORY_METHODS);
    }

    /** */
    @Test
    public void testFactoryMethodReferenceFullName() throws Exception {
        assertCheckstyleFailure("RestrictedFactoryMethodReferenceFullName.java", RESTRICTED_CLASS, RESTRICTED_FACTORY_METHODS);
    }

    /** */
    @Test
    public void testJavaLangClass() throws Exception {
        assertCheckstyleFailure("RestrictedJavaLangClass.java", "java.lang.Thread");
    }

    /** */
    @Test
    public void testNew() throws Exception {
        assertCheckstyleFailure("RestrictedNew.java", RESTRICTED_CLASS);
    }

    /** */
    @Test
    public void testNewFullName() throws Exception {
        assertCheckstyleFailure("RestrictedNewFullName.java", RESTRICTED_CLASS);
    }

    /** */
    @Test
    public void testNewReference() throws Exception {
        assertCheckstyleFailure("RestrictedNewReference.java", RESTRICTED_CLASS);
    }

    /** */
    @Test
    public void testNewReferenceFullName() throws Exception {
        assertCheckstyleFailure("RestrictedNewReferenceFullName.java", RESTRICTED_CLASS);
    }

    /** */
    @Test
    public void testMultipleViolations() throws Exception {
        List<AuditEvent> violations = executeCheckstyleRule("MultipleRestrictedClassUsageRuleViolations.java", RESTRICTED_CLASS, RESTRICTED_FACTORY_METHODS);

        assertEquals(5, violations.size());

    }

    /** */
    @Test(expected = IllegalStateException.class)
    public void testInitFailsWithoutClassName() {
        ClassUsageRestrictionRule rule = new ClassUsageRestrictionRule();

        rule.setSubstitutionClassName("SomeClass");

        rule.init();
    }

    /** */
    @Test(expected = IllegalStateException.class)
    public void testInitFailsWithoutSubstitutionClassName() {
        ClassUsageRestrictionRule rule = new ClassUsageRestrictionRule();

        rule.setClassName(RESTRICTED_CLASS);

        rule.init();
    }

    /** */
    @Test(expected = IllegalStateException.class)
    public void testInitFailsWithSimpleClassName() {
        ClassUsageRestrictionRule rule = new ClassUsageRestrictionRule();

        rule.setClassName("ForkJoinPool");
        rule.setSubstitutionClassName("SomeClass");

        rule.init();
    }

    /** */
    private void assertCheckstyleFailure(String inputFileName, String className, String... factoryMethods) throws Exception {
        List<AuditEvent> violations = executeCheckstyleRule(inputFileName, className, factoryMethods);

        assertEquals(1, violations.size());

        assertEquals(
            "Usage of " + className + " class and its factory methods is restricted." +
                " Use " + SUBSTITUTION_CLASS + " class as a substitution",
            violations.get(0).getMessage());
    }

    /** */
    private List<AuditEvent> executeCheckstyleRule(
        String inputFileName,
        String restrictedClassName,
        String... factoryMethods
    ) throws Exception {
        Checker checker = new Checker();

        try {
            checker.setModuleClassLoader(Thread.currentThread().getContextClassLoader());
            checker.configure(checkerConfiguration(restrictedClassName, factoryMethods));

            List<AuditEvent> violations = new ArrayList<>();

            checker.addListener(new AuditListener() {
                @Override public void auditStarted(AuditEvent evt) {
                }

                @Override public void auditFinished(AuditEvent evt) {
                }

                @Override public void fileStarted(AuditEvent evt) {
                }

                @Override public void fileFinished(AuditEvent evt) {
                }

                @Override public void addError(AuditEvent evt) {
                    violations.add(evt);
                }

                @Override public void addException(AuditEvent evt, Throwable throwable) {
                }
            });

            URL inputFile = getClass().getResource(inputFileName);

            assertNotNull(inputFile);

            checker.process(Collections.singletonList(new File(inputFile.toURI())));

            return violations;

        }
        finally {
            checker.destroy();
        }
    }

    /** */
    private DefaultConfiguration customCheckStyleRuleConfiguration(String restrictedClassName , String... restrictedFactoryMethods) {
        DefaultConfiguration cfg = new DefaultConfiguration(ClassUsageRestrictionRule.class.getName());

        cfg.addProperty("className", restrictedClassName);
        cfg.addProperty("substitutionClassName", SUBSTITUTION_CLASS);

        if (restrictedFactoryMethods.length != 0)
            cfg.addProperty("factoryMethods", String.join(", ", restrictedFactoryMethods));

        return cfg;
    }

    /** */
    private DefaultConfiguration treeWalkerConfiguration(String restrictedClassName , String... restrictedFactoryMethods) {
        DefaultConfiguration cfg = new DefaultConfiguration("TreeWalker");

        cfg.addChild(customCheckStyleRuleConfiguration(restrictedClassName, restrictedFactoryMethods));

        return cfg;
    }

    /** */
    private DefaultConfiguration checkerConfiguration(String restrictedClassName , String... restrictedFactoryMethods) {
        DefaultConfiguration cfg = new DefaultConfiguration("Checker");

        cfg.addProperty("charset", "UTF-8");

        cfg.addChild(treeWalkerConfiguration(restrictedClassName, restrictedFactoryMethods));

        return cfg;
    }
}
