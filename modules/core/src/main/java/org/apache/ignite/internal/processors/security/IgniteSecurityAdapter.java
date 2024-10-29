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

import java.security.CodeSource;
import java.security.ProtectionDomain;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.GridProcessorAdapter;
import org.apache.ignite.internal.util.typedef.F;

import static org.apache.ignite.internal.processors.security.SecurityUtils.doPrivileged;
import static org.apache.ignite.internal.processors.security.SecurityUtils.isInIgnitePackage;

/** */
public abstract class IgniteSecurityAdapter extends GridProcessorAdapter implements IgniteSecurity {
    /** Code source for ignite-core module. */
    private static final CodeSource CORE_CODE_SOURCE = SecurityUtils.class.getProtectionDomain().getCodeSource();

    /** System types cache. */
    private static final ConcurrentMap<Class<?>, Boolean> SYSTEM_TYPES = new ConcurrentHashMap<>();

    /** @param ctx Kernal context. */
    protected IgniteSecurityAdapter(GridKernalContext ctx) {
        super(ctx);
    }

    /** {@inheritDoc} */
    @Override public boolean isSystemType(Class<?> cls) {
        return SYSTEM_TYPES.computeIfAbsent(
            cls,
            c -> {
                ProtectionDomain pd = doPrivileged(c::getProtectionDomain);

                return pd != null
                    && F.eq(CORE_CODE_SOURCE, pd.getCodeSource())
                    // It allows users create an Uber-JAR that includes both Ignite source code and custom classes
                    // and to pass mentioned classes to Ignite via public API (e.g. tasks execution).
                    // Otherwise, Ignite will treat custom classes as internal and block their execution through the
                    // public API.
                    && isInIgnitePackage(cls);
            }
        );
    }
}
