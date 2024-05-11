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
import de.kp.works.ignite.IgniteConstants;

import java.util.HashMap;
import java.util.Map;

import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.util.reference.ReferenceVertexProperty;

public class IgniteEdgeQuery extends IgniteQuery {
    /**
     * Retrieve the edge that refers to the provided
     * `from` and `to `identifier. A use case for this
     * query is the OpenCTI transformer
     */
    public IgniteEdgeQuery(String cacheName, IgniteAdmin admin, Object fromId, Object toId) {
        super(cacheName, admin);
        /*
         * Transform the provided properties into fields
         */
        HashMap<String, String> fields = new HashMap<>();
        
        fields.put(IgniteConstants.FROM_COL_NAME, fromId.toString());
        fields.put(IgniteConstants.TO_COL_NAME,   toId.toString());
        
        if (fromId instanceof ReferenceVertexProperty) {
			ReferenceVertexProperty<?> key = (ReferenceVertexProperty) fromId;			
            fields.put(IgniteConstants.FROM_COL_NAME, key.id().toString());           
            fields.put(IgniteConstants.FROM_TYPE_COL_NAME, key.label());
        }
        
        if (toId instanceof ReferenceVertexProperty) {
			ReferenceVertexProperty<?> key = (ReferenceVertexProperty) toId;
			fields.put(IgniteConstants.TO_COL_NAME, key.id().toString());
            fields.put(IgniteConstants.TO_TYPE_COL_NAME, key.label());
        }
        
        createSql(fields);

    }

    @Override
    protected void createSql(Map<String, String> fields) {

        try {
            buildSelectPart();
            /*
             * Build the `where` clause and thereby distinguish
             * between a single or multiple identifier values
             */
            sqlStatement += " where " + IgniteConstants.FROM_COL_NAME;
            sqlStatement += " = '" + fields.get(IgniteConstants.FROM_COL_NAME) + "'";

            sqlStatement += " and " + IgniteConstants.TO_COL_NAME;
            sqlStatement += " = '" + fields.get(IgniteConstants.TO_COL_NAME) + "'";
            
            if(fields.containsKey(IgniteConstants.FROM_TYPE_COL_NAME)){
                sqlStatement += " and " + IgniteConstants.FROM_TYPE_COL_NAME;
                String id_type = fields.get(IgniteConstants.FROM_TYPE_COL_NAME);
                sqlStatement += " = '" + id_type + "'";
            }
            if(fields.containsKey(IgniteConstants.TO_TYPE_COL_NAME)){
                sqlStatement += " and " + IgniteConstants.TO_TYPE_COL_NAME;
                String id_type = fields.get(IgniteConstants.TO_TYPE_COL_NAME);
                sqlStatement += " = '" + id_type + "'";
            }

        } catch (Exception e) {
            sqlStatement = null;
        }

    }
}
