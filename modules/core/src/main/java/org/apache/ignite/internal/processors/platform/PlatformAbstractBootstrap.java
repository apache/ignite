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

package org.apache.ignite.internal.processors.platform;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgnitionEx;
import org.apache.ignite.internal.processors.platform.memory.PlatformExternalMemory;
import org.apache.ignite.internal.processors.platform.memory.PlatformInputStream;
import org.apache.ignite.internal.processors.resource.GridSpringResourceContext;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteClosure;
import org.jetbrains.annotations.Nullable;

/**
 * Base interop bootstrap implementation.
 */
public abstract class PlatformAbstractBootstrap implements PlatformBootstrap {
    /** {@inheritDoc} */
    @Override public PlatformProcessor start(IgniteConfiguration cfg, @Nullable GridSpringResourceContext springCtx,
        long envPtr) {

        IgniteConfiguration cfg0 = closure(envPtr).apply(cfg);

        try {
            IgniteEx node = (IgniteEx)IgnitionEx.start(cfg0, springCtx);

            return node.context().platform();
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public void init(long dataPtr) {
        final PlatformInputStream input = new PlatformExternalMemory(null, dataPtr).input();

        Ignition.setClientMode(input.readBoolean());

        processInput(input);
    }

    /**
     * Get configuration transformer closure.
     *
     * @param envPtr Environment pointer.
     * @return Closure.
     */
    protected abstract IgniteClosure<IgniteConfiguration, IgniteConfiguration> closure(long envPtr);

    /**
     * Processes any additional input data.
     *
     * @param input Input stream.
     */
    protected void processInput(PlatformInputStream input) {
        // No-op.
    }
}
