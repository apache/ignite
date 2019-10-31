/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.bnf.context;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import org.h2.util.New;

/**
 * Contains meta data information about a procedure.
 * This class is used by the H2 Console.
 */
public class DbProcedure {

    private final DbSchema schema;
    private final String name;
    private final String quotedName;
    private final boolean returnsResult;
    private DbColumn[] parameters;

    public DbProcedure(DbSchema schema, ResultSet rs) throws SQLException {
        this.schema = schema;
        name = rs.getString("PROCEDURE_NAME");
        returnsResult = rs.getShort("PROCEDURE_TYPE") ==
                DatabaseMetaData.procedureReturnsResult;
        quotedName = schema.getContents().quoteIdentifier(name);
    }

    /**
     * @return The schema this table belongs to.
     */
    public DbSchema getSchema() {
        return schema;
    }

    /**
     * @return The column list.
     */
    public DbColumn[] getParameters() {
        return parameters;
    }

    /**
     * @return The table name.
     */
    public String getName() {
        return name;
    }

    /**
     * @return The quoted table name.
     */
    public String getQuotedName() {
        return quotedName;
    }

    /**
     * @return True if this function return a value
     */
    public boolean isReturnsResult() {
        return returnsResult;
    }

    /**
     * Read the column for this table from the database meta data.
     *
     * @param meta the database meta data
     */
    void readParameters(DatabaseMetaData meta) throws SQLException {
        ResultSet rs = meta.getProcedureColumns(null, schema.name, name, null);
        ArrayList<DbColumn> list = New.arrayList();
        while (rs.next()) {
            DbColumn column = DbColumn.getProcedureColumn(schema.getContents(), rs);
            if (column.getPosition() > 0) {
                // Not the return type
                list.add(column);
            }
        }
        rs.close();
        parameters = new DbColumn[list.size()];
        // Store the parameter in the good position [1-n]
        for (int i = 0; i < parameters.length; i++) {
            DbColumn column = list.get(i);
            if (column.getPosition() > 0
                    && column.getPosition() <= parameters.length) {
                parameters[column.getPosition() - 1] = column;
            }
        }
    }

}
