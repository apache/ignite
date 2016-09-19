package org.apache.ignite.internal.processors.platform.entityframework;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.binary.BinaryRawReaderEx;
import org.apache.ignite.internal.processors.platform.cache.PlatformCache;
import org.apache.ignite.internal.processors.platform.cache.PlatformCacheExtension;
import org.apache.ignite.internal.processors.platform.memory.PlatformMemory;
import org.apache.ignite.internal.util.typedef.internal.S;

import java.util.HashSet;

/**
 * EntityFramework cache extension.
 */
public class PlatformDotNetEntityFrameworkCacheExtension implements PlatformCacheExtension {
    /** Extension ID. */
    private static final int EXT_ID = 0;

    /** Operation: increment entity set versions. */
    private static final int OP_INCREMENT_VERSIONS = 1;

    /** {@inheritDoc} */
    @Override public int id() {
        return EXT_ID;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public long processInOutStreamLong(PlatformCache target, int type, BinaryRawReaderEx reader,
        PlatformMemory mem) throws IgniteCheckedException {
        switch (type) {
            case OP_INCREMENT_VERSIONS: {
                HashSet keys = null;
                target.rawCache().invokeAll(keys, new PlatformDotNetEntityFrameworkIncreaseVersionProcessor());

                return target.writeResult(mem, null);
            }

            // TODO: Broadcast cleanup from here.
        }

        throw new IgniteCheckedException("Unsupported operation type: " + type);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(PlatformDotNetEntityFrameworkCacheExtension.class, this);
    }
}
