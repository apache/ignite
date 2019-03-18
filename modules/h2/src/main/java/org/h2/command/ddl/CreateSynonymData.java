/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.command.ddl;

import org.h2.engine.Session;
import org.h2.schema.Schema;

/**
 * The data required to create a synonym.
 */
public class CreateSynonymData {

    /**
     * The schema.
     */
    public Schema schema;

    /**
     * The synonyms name.
     */
    public String synonymName;

    /**
     * The name of the table the synonym is created for.
     */
    public String synonymFor;

    /** Schema synonymFor is located in. */
    public Schema synonymForSchema;

    /**
     * The object id.
     */
    public int id;

    /**
     * The session.
     */
    public Session session;

}
