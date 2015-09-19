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

import org.apache.ignite.internal.processors.platform.PlatformConfigurationEx;
import org.apache.ignite.internal.processors.platform.callback.PlatformCallbackGateway;
import org.apache.ignite.internal.processors.platform.memory.PlatformMemoryManagerImpl;
import org.apache.ignite.internal.processors.platform.utils.PlatformUtils;

import java.util.Collection;

/**
 * Extended .Net configuration.
 */
public class PlatformDotNetConfigurationEx extends PlatformDotNetConfiguration implements PlatformConfigurationEx {
    /** Native gateway. */
    private final PlatformCallbackGateway gate;

    /** Memory manager. */
    private final PlatformMemoryManagerImpl memMgr;

    /** Warnings */
    private Collection<String> warnings;

    /**
     * Copy constructor.
     *
     * @param cfg Configuration to copy.
     * @param gate Native gateway.
     * @param memMgr Memory manager.
     */
    public PlatformDotNetConfigurationEx(PlatformDotNetConfiguration cfg, PlatformCallbackGateway gate,
        PlatformMemoryManagerImpl memMgr) {
        super(cfg);

        this.gate = gate;
        this.memMgr = memMgr;
    }

    /** {@inheritDoc} */
    @Override public PlatformCallbackGateway gate() {
        return gate;
    }

    /** {@inheritDoc} */
    @Override public PlatformMemoryManagerImpl memory() {
        return memMgr;
    }

    /** {@inheritDoc} */
    @Override public String platform() {
        return PlatformUtils.PLATFORM_DOTNET;
    }

    /** {@inheritDoc} */
    @Override public Collection<String> warnings() {
        return warnings;
    }

    /**
     * @param warnings Warnings.
     */
    public void warnings(Collection<String> warnings) {
        this.warnings = warnings;
    }

    /**
     * Unwrap extended configuration.
     *
     * @return Original configuration.
     */
    public PlatformDotNetConfiguration unwrap() {
        return new PlatformDotNetConfiguration(this);
    }
}