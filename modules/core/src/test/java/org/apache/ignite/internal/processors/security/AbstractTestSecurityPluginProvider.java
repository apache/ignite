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

package org.apache.ignite.internal.processors.security;

import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.plugin.AbstractTestPluginProvider;
import org.apache.ignite.plugin.PluginContext;
import org.jetbrains.annotations.Nullable;

/**
 * Security processor provider for tests.
 */
public abstract class AbstractTestSecurityPluginProvider extends AbstractTestPluginProvider {
    /** {@inheritDoc} */
    @Override public String name() {
        return "TestSecurityProcessorProvider";
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public @Nullable Object createComponent(PluginContext ctx, Class cls) {
        if (cls.isAssignableFrom(GridSecurityProcessor.class))
            return securityProcessor(((IgniteEx)ctx.grid()).context());

        return null;
    }

    /**
     * @param ctx Grid kernal context.
     * @return {@link GridSecurityProcessor} istance.
     */
    protected abstract GridSecurityProcessor securityProcessor(GridKernalContext ctx);
}
