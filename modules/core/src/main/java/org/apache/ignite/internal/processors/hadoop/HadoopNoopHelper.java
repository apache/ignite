package org.apache.ignite.internal.processors.hadoop;

import java.io.InputStream;
import org.apache.ignite.internal.GridKernalContext;
import org.jetbrains.annotations.Nullable;

/**
 * Noop Hadoop Helper implementation.
 */
public class HadoopNoopHelper implements HadoopHelper {
    /** {@inheritDoc} */
    @Override public boolean hasExternalDependencies(String clsName, ClassLoader parentClsLdr) {
        throw unsupported();
    }

    /** {@inheritDoc} */
    @Override public byte[] loadReplace(InputStream in, String originalName, String replaceName) {
        throw unsupported();
    }

    /** {@inheritDoc} */
    @Override public boolean isHadoop(String cls) {
        throw unsupported();
    }

    /** {@inheritDoc} */
    @Override public boolean isHadoopIgfs(String cls) {
        throw unsupported();
    }

    /** {@inheritDoc} */
    @Nullable @Override public InputStream loadClassBytes(ClassLoader ldr, String clsName) {
        throw unsupported();
    }

    /**
     * @return Exception.
     */
    private static UnsupportedOperationException unsupported() {
        throw new UnsupportedOperationException("Operation is unsupported (Hadoop module is not in the classpath).");
    }
}
