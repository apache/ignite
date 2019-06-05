/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.mvstore;

/**
 * A position in a cursor
 */
public class CursorPos {

    /**
     * The current page.
     */
    public Page page;

    /**
     * The current index.
     */
    public int index;

    /**
     * The position in the parent page, if any.
     */
    public final CursorPos parent;

    public CursorPos(Page page, int index, CursorPos parent) {
        this.page = page;
        this.index = index;
        this.parent = parent;
    }

}

