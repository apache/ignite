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

package org.apache.ignite.cli;

import javax.inject.Inject;
import javax.inject.Singleton;
import io.micronaut.core.annotation.Introspected;
import picocli.CommandLine;

/**
 * Version provider for Picocli interactions.
 */
@Singleton
@Introspected
public class VersionProvider implements CommandLine.IVersionProvider {

    /** Actual Ignite CLI version info. */
    private final CliVersionInfo cliVerInfo;

    /**
     * Creates version provider.
     *
     * @param cliVerInfo Actual Ignite CLI version container.
     */
    @Inject
    public VersionProvider(CliVersionInfo cliVerInfo) {
        this.cliVerInfo = cliVerInfo;
    }

    /** {@inheritDoc} */
    @Override public String[] getVersion() {
        return new String[] { "Apache Ignite CLI ver. " + cliVerInfo.ver};
    }
}
