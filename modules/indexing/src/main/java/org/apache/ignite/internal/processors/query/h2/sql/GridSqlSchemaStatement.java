/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.query.h2.sql;

import java.lang.reflect.Field;

import org.h2.command.Parser;
import org.h2.command.ddl.SchemaCommand;
import org.h2.schema.Schema;

/**
 * some schema sql statement can native exec in all node. such as: create alias
 * add@byron
 */
public class GridSqlSchemaStatement extends GridSqlStatement {   

    /** Schema name. */
    private String schemaName;

    /** Attempt to drop the index only if it exists. */
    private boolean ifExists;
    
    private final SchemaCommand cmd;
    
    public SchemaCommand getCmd() {
    	return cmd;
	}

	public GridSqlSchemaStatement(SchemaCommand cmd){
    	this.cmd = cmd;  
    	Field getSchema;
		try {
			getSchema = SchemaCommand.class.getDeclaredField("schema");
			getSchema.setAccessible(true);
	    	this.schemaName = ((Schema)getSchema.get(cmd)).getName();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
    }

    /**
     * @return Schema name.
     */
    public String schemaName() {
    	if(schemaName==null) return "PUBLIC";
        return schemaName;
    }

    /**
     * @param schemaName Schema name.
     */
    public void schemaName(String schemaName) {
        this.schemaName = schemaName;
    }

    /**
     * @return whether attempt to drop the index should be made only if it exists.
     */
    public boolean ifExists() {
        return ifExists;
    }

    /**
     * @param ifExists whether attempt to drop the index should be made only if it exists.
     */
    public void ifExists(boolean ifExists) {
        this.ifExists = ifExists;
    }

    /** {@inheritDoc} */
    @Override public String getSQL() {
        return  cmd.toString();
    }
}
