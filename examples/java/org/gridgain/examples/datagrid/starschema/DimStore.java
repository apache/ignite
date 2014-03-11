/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.examples.datagrid.starschema;

import org.gridgain.grid.cache.*;
import org.gridgain.grid.cache.query.*;

/**
 * Represents a physical store location. In our {@code snowflake} schema a {@code store}
 * is a {@code 'dimension'} and will be cached in {@link GridCacheMode#REPLICATED}
 * cache.
 */
public class DimStore {
    /** Primary key. */
    @GridCacheQuerySqlField(unique = true)
    private int id;

    /** Store name. */
    @GridCacheQuerySqlField
    private String name;

    /** Zip code. */
    private String zip;

    /** Address. */
    private String addr;

    /**
     * Constructs a store instance.
     *
     * @param id Store ID.
     * @param name Store name.
     * @param zip Store zip code.
     * @param addr Store address.
     */
    public DimStore(int id, String name, String zip, String addr) {
        this.id = id;
        this.name = name;
        this.zip = zip;
        this.addr = addr;
    }

    /**
     * Gets store ID.
     *
     * @return Store ID.
     */
    public int getId() {
        return id;
    }

    /**
     * Gets store name.
     *
     * @return Store name.
     */
    public String getName() {
        return name;
    }

    /**
     * Gets store zip code.
     *
     * @return Store zip code.
     */
    public String getZip() {
        return zip;
    }

    /**
     * Gets store address.
     *
     * @return Store address.
     */
    public String getAddress() {
        return addr;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        StringBuilder sb = new StringBuilder();

        sb.append("DimStore ");
        sb.append("[id=").append(id);
        sb.append(", name=").append(name);
        sb.append(", zip=").append(zip);
        sb.append(", addr=").append(addr);
        sb.append(']');

        return sb.toString();
    }
}
