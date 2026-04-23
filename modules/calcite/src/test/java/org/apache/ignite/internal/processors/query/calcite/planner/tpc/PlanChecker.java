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

package org.apache.ignite.internal.processors.query.calcite.planner.tpc;

import java.io.IOException;
import java.lang.annotation.Annotation;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.runner.Runner;
import org.junit.runner.notification.RunNotifier;
import org.junit.runners.Suite;
import org.junit.runners.model.InitializationError;
import org.junit.runners.parameterized.BlockJUnit4ClassRunnerWithParameters;
import org.junit.runners.parameterized.TestWithParameters;

/** */
public class PlanChecker extends Suite {
    /** */
    private final List<Runner> runners;

    /**
     * Only called reflectively. Do not use programmatically.
     */
    public PlanChecker(Class<?> klass) throws Throwable {
        super(klass, Collections.emptyList());

        runners = createRunnersForParameters().collect(Collectors.toList());
    }

    /** {@inheritDoc} */
    @Override protected List<Runner> getChildren() {
        return runners;
    }

    /** */
    private Stream<Runner> createRunnersForParameters() throws IOException {
        Stream<String> queries = Files.list(Path.of("./src/test/resources", TpchHelper.sqlTestName(getTestClass().getJavaClass())))
            .filter(p -> p.toString().endsWith(".sql"))
            .sorted()
            .map(p -> p.getFileName().toString().replace(".sql", ""));

        return queries
            .map(qryId -> new TestWithParameters("[queryId=" + qryId + "]", getTestClass(), Collections.singletonList(qryId)))
            .map(test -> {
                try {
                    return new BlockJUnit4ClassRunnerWithParameters(test);
                }
                catch (InitializationError e) {
                    throw new RuntimeException(e);
                }
            });
    }

    /** {@inheritDoc} */
    @Override public void run(RunNotifier notifier) {
        runAnnotated(BeforePlansTest.class);
        try {
            super.run(notifier);
        }
        finally {
            runAnnotated(AfterPlansTest.class);
        }
    }

    private void runAnnotated(Class<? extends Annotation> annotation) {
        getTestClass().getAnnotatedMethods(annotation).forEach(m -> {
            List<Throwable> errors = new ArrayList<>();

            m.validatePublicVoid(true, errors);

            try {
                if (!errors.isEmpty())
                    throw errors.get(0);

                m.invokeExplosively(getTestClass().getJavaClass(), getTestClass().getJavaClass());
            }
            catch (Throwable e) {
                throw new RuntimeException(e);
            }
        });
    }

    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.TYPE)
    public @interface PlansTest {
        String name() default "";

        Class<? extends Enum<? extends TpcTable>> tables();
    }

    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.METHOD)
    public @interface BeforePlansTest {
    }

    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.METHOD)
    public @interface AfterPlansTest {
        String name() default "";
    }
}
