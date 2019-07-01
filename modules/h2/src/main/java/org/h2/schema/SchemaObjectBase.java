/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.schema;

import org.h2.engine.DbObjectBase;

/**
 * The base class for classes implementing SchemaObject.
 */
public abstract class SchemaObjectBase extends DbObjectBase implements
        SchemaObject {

    private Schema schema;

    /**
     * Initialize some attributes of this object.
     *
     * @param newSchema the schema
     * @param id the object id
     * @param name the name
     * @param traceModuleId the trace module id
     */
    protected void initSchemaObjectBase(Schema newSchema, int id, String name,
            int traceModuleId) {
        initDbObjectBase(newSchema.getDatabase(), id, name, traceModuleId);
        this.schema = newSchema;
    }

    @Override
    public Schema getSchema() {
        return schema;
    }

    @Override
    public String getSQL() {
        return schema.getSQL() + "." + super.getSQL();
    }

    @Override
    public boolean isHidden() {
        return false;
    }

}
