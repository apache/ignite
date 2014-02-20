// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.examples.datagrid;

import org.gridgain.grid.cache.query.*;
import org.gridgain.grid.product.*;

import java.io.*;
import java.util.*;

import static org.gridgain.grid.product.GridProductEdition.*;

/**
 * Organization record used for query examples.
 *
 * @author @java.author
 * @version @java.version
 */
@GridOnlyAvailableIn(DATA_GRID)
public class Organization implements Serializable {
    /** Organization ID (create unique SQL-based index for this field). */
    @GridCacheQuerySqlField(unique = true)
    private UUID id;

    /** Organization name (create non-unique SQL-based index for this field. */
    @GridCacheQuerySqlField
    private String name;

    /**
     * Constructs organization with generated ID.
     */
    public Organization() {
        id = UUID.randomUUID();
    }

    /**
     * Constructs organization with given ID.
     *
     * @param id Organization ID.
     */
    public Organization(UUID id) {
        this.id = id;
    }

    /**
     * Create organization.
     *
     * @param name Organization name.
     */
    public Organization(String name) {
        id = UUID.randomUUID();

        this.name = name;
    }

    /**
     * @return Organization id.
     */
    public UUID getId() {
        return id;
    }

    /**
     * @param id Organization id.
     */
    public void setId(UUID id) {
        this.id = id;
    }

    /**
     * @return Organization name.
     */
    public String getName() {
        return name;
    }

    /**
     * @param name Organization name.
     */
    public void setName(String name) {
        this.name = name;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        return this == o || (o instanceof Organization) && id.equals(((Organization)o).id);

    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return id.hashCode();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        StringBuilder sb = new StringBuilder();

        sb.append("Organization ");
        sb.append("[id=").append(id);
        sb.append(", name=").append(name);
        sb.append(']');

        return sb.toString();
    }
}
