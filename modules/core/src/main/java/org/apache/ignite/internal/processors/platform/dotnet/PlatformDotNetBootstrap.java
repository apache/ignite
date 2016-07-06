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

package org.apache.ignite.internal.processors.platform.dotnet;

import org.apache.ignite.internal.processors.platform.PlatformAbstractBootstrap;
import org.apache.ignite.internal.processors.platform.PlatformAbstractConfigurationClosure;

import java.io.PrintStream;

/**
 * Interop .Net bootstrap.
 */
public class PlatformDotNetBootstrap extends PlatformAbstractBootstrap {
    /** {@inheritDoc} */
    @Override public void init() {
        // Initialize console propagation.
        // This call is idempotent, doing it on each node start is fine.
        System.setOut(new PrintStream(new PlatformDotNetConsoleStream(false)));
        System.setErr(new PrintStream(new PlatformDotNetConsoleStream(true)));

        super.init();
    }

    /** {@inheritDoc} */
    @Override protected PlatformAbstractConfigurationClosure closure(long envPtr) {
        return new PlatformDotNetConfigurationClosure(envPtr);
    }
}