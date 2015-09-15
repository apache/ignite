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

package org.apache.ignite.internal.processors.cache.portable;

import java.util.Collection;
import org.apache.ignite.internal.portable.api.IgnitePortables;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.cacheobject.IgniteCacheObjectProcessor;
import org.apache.ignite.internal.portable.api.PortableBuilder;
import org.apache.ignite.internal.portable.api.PortableException;
import org.apache.ignite.internal.portable.api.PortableMetadata;
import org.apache.ignite.internal.portable.api.PortableObject;
import org.jetbrains.annotations.Nullable;

/**
 * {@link IgnitePortables} implementation.
 */
public class IgnitePortablesImpl implements IgnitePortables {
    /** */
    private GridKernalContext ctx;

    /** */
    private CacheObjectPortableProcessor proc;

    /**
     * @param ctx Context.
     */
    public IgnitePortablesImpl(GridKernalContext ctx, CacheObjectPortableProcessor proc) {
        this.ctx = ctx;

        this.proc = proc;
    }

    /** {@inheritDoc} */
    @Override public int typeId(String typeName) {
        guard();

        try {
            return proc.typeId(typeName);
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @Override public <T> T toPortable(@Nullable Object obj) throws PortableException {
        guard();

        try {
            return (T)proc.marshalToPortable(obj);
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @Override public PortableBuilder builder(int typeId) {
        guard();

        try {
            return proc.builder(typeId);
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @Override public PortableBuilder builder(String typeName) {
        guard();

        try {
            return proc.builder(typeName);
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @Override public PortableBuilder builder(PortableObject portableObj) {
        guard();

        try {
            return proc.builder(portableObj);
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @Nullable @Override public PortableMetadata metadata(Class<?> cls) throws PortableException {
        guard();

        try {
            return proc.metadata(proc.typeId(cls.getName()));
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @Nullable @Override public PortableMetadata metadata(String typeName) throws PortableException {
        guard();

        try {
            return proc.metadata(proc.typeId(typeName));
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @Nullable @Override public PortableMetadata metadata(int typeId) throws PortableException {
        guard();

        try {
            return proc.metadata(typeId);
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @Override public Collection<PortableMetadata> metadata() throws PortableException {
        guard();

        try {
            return proc.metadata();
        }
        finally {
            unguard();
        }
    }

    /**
     * @return Portable processor.
     */
    public IgniteCacheObjectProcessor processor() {
        return proc;
    }

    /**
     * <tt>ctx.gateway().readLock()</tt>
     */
    private void guard() {
        ctx.gateway().readLock();
    }

    /**
     * <tt>ctx.gateway().readUnlock()</tt>
     */
    private void unguard() {
        ctx.gateway().readUnlock();
    }
}