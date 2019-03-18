/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.build.indexer;

/**
 * Represents a page of the indexer.
 */
public class Page {

    /**
     * The page id.
     */
    int id;

    /**
     * The file name.
     */
    final String fileName;

    /**
     * The title of the page.
     */
    String title;

    /**
     * The total weight of this page.
     */
    // TODO page.totalWeight is currently not used
    int totalWeight;

    /**
     * The number of relations between a page and a word.
     */
    int relations;

    Page(int id, String fileName) {
        this.id = id;
        this.fileName = fileName;
    }

    @Override
    public String toString() {
        return "p" + id + "(" + fileName + ")";
    }

}
