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

package org.apache.ignite.internal.processors.query;

import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.binary.BinaryMarshaller;
import org.apache.ignite.internal.processors.cache.CacheObjectValueContext;

/**
 * Cache object value context for queries.
 */
public class CacheQueryObjectValueContext implements CacheObjectValueContext {
    /** Kernal context. */
    private final GridKernalContext ctx;

    /**
     * Constructor.
     *
     * @param ctx Kernal context.
     */
    public CacheQueryObjectValueContext(GridKernalContext ctx) {
        this.ctx = ctx;
    }

    /** {@inheritDoc} */
    @Override public GridKernalContext kernalContext() {
        return ctx;
    }

    /** {@inheritDoc} */
    @Override public boolean copyOnGet() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean storeValue() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean addDeploymentInfo() {
        return ctx.config().isPeerClassLoadingEnabled() && !binaryEnabled();
    }

    /** {@inheritDoc} */
    @Override public boolean binaryEnabled() {
        return ctx.config().getMarshaller() instanceof BinaryMarshaller;
    }
}
