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

package org.apache.ignite.util;

import org.apache.ignite.internal.management.api.Command;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

/**
 * Dynamically registers test command classes for individual test methods based on the
 * {@link RegisterTestCommands} annotation.
 */
public class RegisterTestCommandsRule implements TestRule {
    /** {@inheritDoc} */
    @Override public Statement apply(Statement base, Description description) {
        return new Statement() {
            /** {@inheritDoc} */
            @Override public void evaluate() throws Throwable {
                RegisterTestCommands annotation = description.getAnnotation(RegisterTestCommands.class);

                if (annotation != null) {
                    try {
                        for (Class<? extends Command<?, ?>> cmdCls : annotation.value())
                            TestCommandsProvider.registerCommand(cmdCls);

                        base.evaluate();
                    }
                    finally {
                        TestCommandsProvider.unregisterAll();
                    }
                }
                else
                    base.evaluate();
            }
        };
    }
}
