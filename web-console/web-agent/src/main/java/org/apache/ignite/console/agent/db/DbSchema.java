

package org.apache.ignite.console.agent.db;

import java.util.Collection;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Database schema names with catalog name.
 */
public class DbSchema {
    /** Catalog name. */
    private final String catalog;

    /** Schema names. */
    private final Collection<String> schemas;

    /**
     * @param catalog Catalog name.
     * @param schemas Schema names.
     */
    public DbSchema(String catalog, Collection<String> schemas) {
        this.catalog = catalog;
        this.schemas = schemas;
    }

    /**
     * @return Catalog name.
     */
    public String getCatalog() {
        return catalog;
    }

    /**
     * @return Schema names.
     */
    public Collection<String> getSchemas() {
        return schemas;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(DbSchema.class, this);
    }
}
