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

package org.apache.ignite.internal.processors.hadoop;

import org.apache.ignite.internal.GridKernalContext;
import org.jetbrains.annotations.Nullable;

import java.io.InputStream;

/**
 * Noop Hadoop Helper implementation.
 */
@SuppressWarnings("unused")
public class HadoopNoopHelper implements HadoopHelper {
    /**
     * Constructor.
     *
     * @param ctx Kernal context.
     */
    public HadoopNoopHelper(GridKernalContext ctx) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public boolean isNoOp() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public HadoopClassLoader commonClassLoader() {
        throw unsupported();
    }

    /** {@inheritDoc} */
    @Override public byte[] loadReplace(InputStream in, String originalName, String replaceName) {
        throw unsupported();
    }

    /** {@inheritDoc} */
    @Nullable @Override public InputStream loadClassBytes(ClassLoader ldr, String clsName) {
        throw unsupported();
    }

    /** {@inheritDoc} */
    @Override public String workDirectory() {
        throw unsupported();
    }

    /** {@inheritDoc} */
    @Override public void close() {
        // No-op.
    }

    /**
     * @return Exception.
     */
    private static UnsupportedOperationException unsupported() {
        throw new UnsupportedOperationException("Operation is unsupported (Hadoop module is not in the classpath).");
    }
}
