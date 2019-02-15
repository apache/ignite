/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.internal.processors.platform.dotnet;

import java.util.List;
import org.apache.ignite.internal.logger.platform.PlatformLogger;
import org.apache.ignite.internal.processors.platform.PlatformConfigurationEx;
import org.apache.ignite.internal.processors.platform.cache.PlatformCacheExtension;
import org.apache.ignite.internal.processors.platform.callback.PlatformCallbackGateway;
import org.apache.ignite.internal.processors.platform.entityframework.PlatformDotNetEntityFrameworkCacheExtension;
import org.apache.ignite.internal.processors.platform.memory.PlatformMemoryManagerImpl;
import org.apache.ignite.internal.processors.platform.utils.PlatformUtils;
import org.apache.ignite.internal.processors.platform.websession.PlatformDotNetSessionCacheExtension;
import org.apache.ignite.platform.dotnet.PlatformDotNetBinaryConfiguration;
import org.apache.ignite.platform.dotnet.PlatformDotNetConfiguration;
import org.jetbrains.annotations.Nullable;

import java.util.ArrayList;
import java.util.Collection;

/**
 * Extended .Net configuration.
 */
public class PlatformDotNetConfigurationEx extends PlatformDotNetConfiguration implements PlatformConfigurationEx {
    /** Native gateway. */
    private final PlatformCallbackGateway gate;

    /** Memory manager. */
    private final PlatformMemoryManagerImpl memMgr;

    /** Logger. */
    private final PlatformLogger logger;

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
        PlatformMemoryManagerImpl memMgr, PlatformLogger logger) {
        super(cfg);

        this.gate = gate;
        this.memMgr = memMgr;
        this.logger = logger;
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

    /** {@inheritDoc} */
    @Nullable @Override public Collection<PlatformCacheExtension> cacheExtensions() {
        Collection<PlatformCacheExtension> exts = new ArrayList<>(2);

        exts.add(new PlatformDotNetSessionCacheExtension());
        exts.add(new PlatformDotNetEntityFrameworkCacheExtension());

        return exts;
    }

    /** {@inheritDoc} */
    @Override public PlatformLogger logger() {
        return logger;
    }

    /** {@inheritDoc} */
    @Override public PlatformDotNetConfigurationEx setBinaryConfiguration(PlatformDotNetBinaryConfiguration binaryCfg) {
        super.setBinaryConfiguration(binaryCfg);

        return this;
    }

    /** {@inheritDoc} */
    @Override public PlatformDotNetConfigurationEx setAssemblies(List<String> assemblies) {
        super.setAssemblies(assemblies);

        return this;
    }

    /**
     * @param warnings Warnings.
     * @return {@code this} for chaining.
     */
    public PlatformDotNetConfigurationEx warnings(Collection<String> warnings) {
        this.warnings = warnings;

        return this;
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