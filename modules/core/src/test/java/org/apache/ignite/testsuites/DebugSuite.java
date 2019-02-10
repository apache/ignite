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

package org.apache.ignite.testsuites;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.stream.IntStream;
import org.jetbrains.annotations.NotNull;
import org.junit.runner.Runner;
import org.junit.runner.notification.RunNotifier;
import org.junit.runners.BlockJUnit4ClassRunner;
import org.junit.runners.Suite;
import org.junit.runners.model.FrameworkMethod;
import org.junit.runners.model.InitializationError;
import org.junit.runners.model.RunnerBuilder;

public class DebugSuite extends Suite {
    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.TYPE)
    public @interface Config {
        Class<?> testClass();
        String method() default "*";
        int times() default 1;
    }

    private final Config cfg;

    public DebugSuite(Class<?> suiteClass, RunnerBuilder builder) throws InitializationError {
        super(filteringRunnerBuilder(suiteClass, builder), testClasses(suiteClass));

        cfg = suiteClass.getAnnotation(Config.class);
    }

    private static RunnerBuilder filteringRunnerBuilder(Class<?> suiteClass, RunnerBuilder builder) {
        Config cfg = debugConfig(suiteClass);

        return new RunnerBuilder() {
            @Override public Runner runnerForClass(Class<?> testClass) throws Throwable {
                Runner dfltRunner = builder.runnerForClass(testClass);
                if (!(dfltRunner instanceof BlockJUnit4ClassRunner))
                    throw new IllegalStateException();

                return new BlockJUnit4ClassRunner(testClass) {
                    @Override protected void runChild(FrameworkMethod method, RunNotifier notifier) {
                        if (cfg.method().equals("*") || cfg.method().equals(method.getName()))
                            super.runChild(method, notifier);
                        else
                            notifier.fireTestIgnored(describeChild(method));
                    }
                };
            }
        };
    }

    private static Class<?>[] testClasses(Class<?> suiteClass) {
        Config cfg = debugConfig(suiteClass);

        return IntStream.range(0, cfg.times())
            .mapToObj(i -> cfg.testClass())
            .toArray(Class[]::new);
    }

    @NotNull private static DebugSuite.Config debugConfig(Class<?> suiteClass) {
        Config cfg = suiteClass.getAnnotation(Config.class);
        if (cfg == null)
            throw new IllegalStateException();

        return cfg;
    }
}
