package org.apache.ignite.internal.processors.cache.persistence.filename;

import org.apache.ignite.IgniteCheckedException;

/**
 * Resolves folders for PDS mode, may have side effect as setting random UUID as node consistent ID
 */
public interface PdsFolderResolver {
    public PdsFolderSettings resolveFolders() throws IgniteCheckedException;
}
