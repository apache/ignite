/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.visor.cmd.dto;

import org.gridgain.grid.kernal.visor.cmd.tasks.*;

import java.io.*;
import java.util.*;

/**
 * Result of {@link VisorNextFieldsQueryPageTask}.
 */
public class VisorFieldsQueryResult implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    private final List<Object[]> rows;

    private final Boolean hasMore;

    /**
     * @param rows First page of rows fetched from query.
     * @param hasMore `True` if there is more data could be fetched.
     */
    public VisorFieldsQueryResult(List<Object[]> rows, Boolean hasMore) {
        this.rows = rows;
        this.hasMore = hasMore;
    }

    /**
     * @return Rows.
     */
    public List<Object[]> rows() {
        return rows;
    }

    /**
     * @return Has more.
     */
    public Boolean hasMore() {
        return hasMore;
    }
}
