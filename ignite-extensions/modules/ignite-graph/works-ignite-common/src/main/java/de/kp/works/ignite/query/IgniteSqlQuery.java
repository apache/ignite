package de.kp.works.ignite.query;
/*
 * Copyright (c) 2019 - 2021 Dr. Krusche & Partner PartG. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 *
 * @author Stefan Krusche, Dr. Krusche & Partner PartG
 *
 */

import de.kp.works.ignite.IgniteAdmin;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;

public class IgniteSqlQuery extends IgniteQuery {
	private Object[] args;
    /**
     * Retrieve all elements that refer to the
     * selected Ignite cache
     */
    public IgniteSqlQuery(String cacheName, IgniteAdmin admin, String sql, Object[] args) {
        super(cacheName, admin);
        this.sqlStatement = sql;
        this.args = args;
        /*
         * This query does not take fields
         */
        Map<String, String> fields = new HashMap<>();
        createSql(fields);
    }
    
    public void setSqlStatment(String sql) {
    	this.sqlStatement = sql;
    }

    @Override
    protected void createSql(Map<String,String> fields) {
        try {
            /*
             * This query does not have a `where` clause
             * and just contains the `select` part
             */
        	if(sqlStatement==null || sqlStatement.isBlank()) {
        		sqlStatement = "select _key, * ";               
                sqlStatement += " from " + cache.getName();
        	}

        } catch (Exception e) {
            sqlStatement = null;
        }

    }
    
    @Override
    public List<IgniteResult> getSqlResultWithMeta() {
        SqlFieldsQuery sqlQuery = new SqlFieldsQuery(sqlStatement);
        sqlQuery.setArgs(this.args);
        
        FieldsQueryCursor<List<?>> cursor = cache.query(sqlQuery);
        List<List<?>> sqlResult = cursor.getAll();
        List<IgniteResult> result = new ArrayList<>();
    	for(List<?> row: sqlResult) {
    		IgniteResult item = new IgniteResult();
    		for(int i=0;i<cursor.getColumnsCount();i++) {
    			Object value = row.get(i);
    			if(value!=null) {
    				String field = cursor.getFieldName(i);
    				if(field.equals("_KEY")) {
    					field = "id";
    				}
    				item.addColumn(field, value.getClass().getSimpleName(), value);
    			}
            }
    		result.add(item);
    	}    	
    	return result;
    }

}
