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

package org.apache.ignite.testframework.junits;

import org.junit.jupiter.api.function.Executable;

/**
 *
 */
public class DelegatingJUnitStatement implements Executable {
    /** Object to delegate executable body to. */
    private Executable delegate;

    /**
     * @param delegate Object to delegate executable body to.
     * @return {@link Executable} as a {@link org.junit.jupiter.api.function.Executable}.
     */
    public static Executable wrap(Executable delegate) {
        return new DelegatingJUnitStatement(delegate);
    }

    /**
     * @param delegate Object to delegate executable body to.
     */
    private DelegatingJUnitStatement(Executable delegate) {
        this.delegate = delegate;
    }

    /**
     * Executes the delegate executable.
     *
     * @throws Throwable if the delegate execution throws an exception
     */
    @Override
    public void execute() throws Throwable {
        delegate.execute();
    }
}
