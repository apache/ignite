package org.apache.ignite.internal.processors.hadoop;

import java.io.InputStream;
import org.apache.ignite.internal.GridKernalContext;
import org.jetbrains.annotations.Nullable;

/**
 * Noop Hadoop Helper implementation.
 */
public class HadoopNoopHelper implements HadoopHelper {
    /** Constructor required by the engine. */
    public HadoopNoopHelper(GridKernalContext ctx) {
        // nool
    }

    /** {@inheritDoc} */
    @Override public boolean hasExternalDependencies(String clsName, ClassLoader parentClsLdr) {
        return false;
    }

    /** {@inheritDoc} */
    @Override public byte[] loadReplace(InputStream in, String originalName, String replaceName) {
        return new byte[0];
    }

    /** {@inheritDoc} */
    @Override public boolean isHadoop(String cls) {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean isHadoopIgfs(String cls) {
        return false;
    }

    /** {@inheritDoc} */
    @Nullable @Override public InputStream loadClassBytes(ClassLoader ldr, String clsName) {
        return null;
    }
}
