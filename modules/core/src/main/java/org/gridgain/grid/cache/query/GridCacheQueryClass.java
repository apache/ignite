// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.cache.query;

import org.gridgain.grid.util.tostring.*;
import org.gridgain.grid.util.typedef.internal.*;

import java.util.*;

/**
 * TODO: Add class description.
 *
 * @author @java.author
 * @version @java.version
 */
public class GridCacheQueryClass {
    /** Type name, e.g. class name. */
    @GridToStringInclude
    private String type;

    /** Fields to be queried, in addition to indexed fields. */
    @GridToStringInclude
    private Collection<String> qryFlds;

    /** Fields to index in ascending order. */
    @GridToStringInclude
    private Collection<String> ascFlds;

    /** Fields to index in descending order. */
    @GridToStringInclude
    private Collection<String> descFlds;

    /** Fields to create group indexes for. */
    @GridToStringInclude
    private Collection<LinkedHashMap<String, Boolean>> grps;

    public String getType() {
        return type;
    }

    public void setType(Class<?> cls) {
        type = cls.getSimpleName();
    }

    public void setType(String type) {
        this.type = type;
    }

    /**
     * Gets queryable fields. If {@code '*'}, then all fields will be queryable.
     *
     * @return
     */
    public Collection<String> getQueryFields() {
        return qryFlds;
    }

    public void setQueryFields(String... qryFlds) {
        this.qryFlds = Arrays.asList(qryFlds);
    }

    public Collection<String> getAscendingFields() {
        return ascFlds;
    }

    public void setAscendingFields(String... ascFlds) {
        this.ascFlds = Arrays.asList(ascFlds);
    }

    public Collection<String> getDescendingFields() {
        return descFlds;
    }

    public void setDescendingFields(String... descFlds) {
        this.descFlds = Arrays.asList(descFlds);
    }

    public Collection<LinkedHashMap<String, Boolean>> getGroups() {
        return grps;
    }

    public void setGroups(LinkedHashMap<String, Boolean>... grps) {
        this.grps = Arrays.asList(grps);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridCacheQueryClass.class, this);
    }
}
