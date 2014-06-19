/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.visor.gui.dto;

import org.gridgain.grid.util.typedef.internal.*;

import java.io.*;
import java.util.*;

/**
 * MongoDB collection range data.
 */
public class VisorMongoRange implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Database name this range belongs to. */
    private final String database;

    /** Collection name this range belongs to. */
    private final String collection;

    /** Range ID. */
    private final Long id;

    /** Number of documents in range. */
    private final Integer size;

    /** Collection of nodes IDs on which range is present. */
    private final List<UUID> nids;

    /**
     * Create MongoDB collection range.
     * 
     * @param database Database name this range belongs to.
     * @param collection Collection name this range belongs to.
     * @param id Range ID.
     * @param size Number of documents in range.
     * @param nids Collection of nodes IDs on which range is present.
     */
    public VisorMongoRange(String database, String collection, Long id, Integer size, List<UUID> nids) {
        this.database = database;
        this.collection = collection;
        this.id = id;
        this.size = size;
        this.nids = nids;
    }

    /**
     * @return Database name this range belongs to.
     */
    public String database() {
        return database;
    }

    /**
     * @return Collection name this range belongs to.
     */
    public String collection() {
        return collection;
    }

    /**
     * @return Range ID.
     */
    public Long id() {
        return id;
    }

    /**
     * @return Number of documents in range.
     */
    public Integer size() {
        return size;
    }

    /**
     * @return Collection of nodes IDs on which range is present.
     */
    public List<UUID> nids() {
        return nids;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(VisorMongoRange.class, this);
    }
}
