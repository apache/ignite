/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.engine;

import org.h2.command.Prepared;

/**
 * Represents a procedure. Procedures are implemented for PostgreSQL
 * compatibility.
 */
public class Procedure {

    private final String name;
    private final Prepared prepared;

    public Procedure(String name, Prepared prepared) {
        this.name = name;
        this.prepared = prepared;
    }

    public String getName() {
        return name;
    }

    public Prepared getPrepared() {
        return prepared;
    }

}
