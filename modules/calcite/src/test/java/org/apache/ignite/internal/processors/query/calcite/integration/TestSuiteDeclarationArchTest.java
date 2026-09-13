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
 *
 */

package org.apache.ignite.internal.processors.query.calcite.integration;

import java.util.Set;
import com.tngtech.archunit.core.domain.JavaClass;
import com.tngtech.archunit.core.domain.JavaClasses;
import com.tngtech.archunit.core.domain.JavaMethod;
import com.tngtech.archunit.core.domain.JavaModifier;
import com.tngtech.archunit.core.importer.ClassFileImporter;
import com.tngtech.archunit.core.importer.ImportOption;
import com.tngtech.archunit.core.importer.Location;
import com.tngtech.archunit.junit.AnalyzeClasses;
import com.tngtech.archunit.junit.ArchTest;
import com.tngtech.archunit.junit.LocationProvider;
import com.tngtech.archunit.lang.ArchCondition;
import com.tngtech.archunit.lang.ArchRule;
import com.tngtech.archunit.lang.ConditionEvents;
import com.tngtech.archunit.lang.SimpleConditionEvent;
import org.junit.platform.suite.api.SelectClasses;
import org.junit.platform.suite.api.Suite;

import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.methods;

/** */
@AnalyzeClasses(
    importOptions = ImportOption.OnlyIncludeTests.class,
    locations = TestSuiteDeclarationArchTest.CalciteLocationProvider.class)
public class TestSuiteDeclarationArchTest {
    /** */
    private static JavaClasses allClassesCache;

    /**
     * Resolves compiled test classes of the calcite module (i.e. {@code modules/calcite/target/test-classes}).
     * The location is derived from the test class itself instead of the working directory, because with
     * {@code forkCount=0} the working directory is the one Maven was started from (e.g. the repository root),
     * which would make the rules scan every module.
     */
    static class CalciteLocationProvider implements LocationProvider {
        /** */
        @Override public Set<Location> get(Class<?> testCls) {
            return Set.of(calciteTestClassesLocation());
        }
    }

    /** @return Location of the calcite module compiled test classes. */
    private static Location calciteTestClassesLocation() {
        return Location.of(TestSuiteDeclarationArchTest.class.getProtectionDomain().getCodeSource().getLocation());
    }

    /** */
    @ArchTest
    @SuppressWarnings("unused")
    static final ArchRule CHECK_LEGACY_TESTS =
        methods()
            .that().areAnnotatedWith(org.junit.Test.class)
            .should(new ArchCondition<JavaMethod>("Legacy JUnit defined test with: @org.junit.Test") {
                @Override public void check(JavaMethod method, ConditionEvents events) {
                    events.add(SimpleConditionEvent.violated(method, "Unexpected test annotation: " + method.getFullName()));
                }
            })
            .allowEmptyShould(true);

    /** */
    @ArchTest
    @SuppressWarnings("unused")
    static final ArchRule CHECK_ALL_TEST_CLASSES_IN_SUITE =
        methods()
            .that().areAnnotatedWith(org.junit.jupiter.api.Test.class)
            .should(new ArchCondition<JavaMethod>("be declared in a class listed in @SelectClasses of a @Suite") {
                /** */
                @Override public void check(JavaMethod method, ConditionEvents events) {
                    // Lazy load all classes to avoid repeated scanning
                    if (allClassesCache == null) {
                        allClassesCache = new ClassFileImporter()
                            .importLocations(Set.of(calciteTestClassesLocation()));
                    }

                    JavaClass ownerCls = method.getOwner();

                    if (ownerCls.getModifiers().stream().anyMatch(m -> m == JavaModifier.ABSTRACT))
                        return;

                    boolean isInSuite = allClassesCache.stream()
                        .filter(cls -> cls.isAnnotatedWith(Suite.class))
                        .anyMatch(suite -> isClassReferencedInSuite(suite, ownerCls));

                    if (!isInSuite) {
                        String msg = String.format(
                            "Method %s#%s is in a class that is not listed in any @SelectClasses.",
                            ownerCls.getFullName(), method.getName()
                        );
                        events.add(SimpleConditionEvent.violated(method, msg));
                    }
                }

                private boolean isClassReferencedInSuite(JavaClass suiteClass, JavaClass testClass) {
                    SelectClasses selectClasses = suiteClass.reflect().getAnnotation(SelectClasses.class);
                    if (selectClasses != null) {
                        for (Class<?> selected : selectClasses.value()) {
                            if (selected.getName().equals(testClass.getFullName())) {
                                return true;
                            }
                        }
                    }
                    return false;
                }
            });
}
