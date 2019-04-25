/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.platform.dotnet;

import org.apache.ignite.internal.processors.platform.PlatformAbstractBootstrap;
import org.apache.ignite.internal.processors.platform.PlatformAbstractConfigurationClosure;
import org.apache.ignite.internal.processors.platform.memory.PlatformInputStream;

import java.io.PrintStream;

/**
 * Interop .Net bootstrap.
 */
public class PlatformDotNetBootstrap extends PlatformAbstractBootstrap {
    private boolean useLogger;

    /** {@inheritDoc} */
    @Override protected PlatformAbstractConfigurationClosure closure(long envPtr) {
        return new PlatformDotNetConfigurationClosure(envPtr, useLogger);
    }

    /** {@inheritDoc} */
    @Override protected void processInput(PlatformInputStream input) {
        useLogger = input.readBoolean();

        if (input.readBoolean()) {
            // Initialize console propagation.
            // This call is idempotent, doing it on each node start is fine.
            System.setOut(new PrintStream(new PlatformDotNetConsoleStream(false)));
            System.setErr(new PrintStream(new PlatformDotNetConsoleStream(true)));
        }
    }
}