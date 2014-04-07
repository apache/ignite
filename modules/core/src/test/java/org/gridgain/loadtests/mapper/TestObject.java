/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.loadtests.mapper;

import org.gridgain.grid.cache.query.*;

import java.io.*;

/**
 * Test object.
 */
public class TestObject implements Serializable {
    /** ID. */
    @GridCacheQuerySqlField(unique = true)
    private int id;

    /** Text. */
    @GridCacheQuerySqlField
    private String txt;

    /**
     * @param id ID.
     * @param txt Text.
     */
    public TestObject(int id, String txt) {
        this.id = id;
        this.txt = txt;
    }

    /**
     * @return ID.
     */
    public int getId() {
        return id;
    }

    /**
     * @return Text.
     */
    public String getText() {
        return txt;
    }
}
