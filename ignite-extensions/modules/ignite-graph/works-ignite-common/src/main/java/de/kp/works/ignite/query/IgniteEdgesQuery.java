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
import org.apache.tinkerpop.gremlin.structure.Direction;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class IgniteEdgesQuery extends IgniteQuery {
	
	Direction direction;

    public IgniteEdgesQuery(String cacheName, IgniteAdmin admin,
                            Object vertex, Direction direction, String... labels) {
        super(cacheName, admin);
        /*
         * Transform the provided properties into fields
         */
        this.direction = direction;
        HashMap<String, String> fields = new HashMap<>();
        vertexToFields(vertex, direction, fields);
        if(labels.length>0)
        	fields.put(IgniteConstants.LABEL_COL_NAME, String.join(",", labels));
        createSql(fields);

    }

    public IgniteEdgesQuery(String cacheName, IgniteAdmin admin,
                            Object vertex, Direction direction, String label, String key, Object value) {
        super(cacheName, admin);
        /*
         * Transform the provided properties into fields
         */
        HashMap<String, String> fields = new HashMap<>();
        vertexToFields(vertex, direction, fields);

        fields.put(IgniteConstants.LABEL_COL_NAME, label);

        fields.put(IgniteConstants.PROPERTY_KEY_COL_NAME, key);
        fields.put(IgniteConstants.PROPERTY_VALUE_COL_NAME, value.toString());

        createSql(fields);

    }

    @Override
    protected void createSql(Map<String, String> fields) {
        try {
            buildSelectPart();            
            
            if (this.direction == Direction.IN && fields.containsKey(IgniteConstants.TO_COL_NAME)) {
                sqlStatement += " where " + IgniteConstants.TO_COL_NAME;
                sqlStatement += " = '" + fields.get(IgniteConstants.TO_COL_NAME) + "'";                
                
                if(fields.containsKey(IgniteConstants.TO_TYPE_COL_NAME)){
                    sqlStatement += " and " + IgniteConstants.TO_TYPE_COL_NAME;
                    String id_type = fields.get(IgniteConstants.TO_TYPE_COL_NAME);
                    sqlStatement += " = '" + id_type + "'";
                }
            }
            else if(this.direction == Direction.OUT && fields.containsKey(IgniteConstants.FROM_COL_NAME)){
                sqlStatement += " where " + IgniteConstants.FROM_COL_NAME;
                sqlStatement += " = '" + fields.get(IgniteConstants.FROM_COL_NAME) + "'";
                
                if(fields.containsKey(IgniteConstants.FROM_TYPE_COL_NAME)){
                    sqlStatement += " and " + IgniteConstants.FROM_TYPE_COL_NAME;
                    String id_type = fields.get(IgniteConstants.FROM_TYPE_COL_NAME);
                    sqlStatement += " = '" + id_type + "'";
                }
            }
            else if(this.direction == Direction.BOTH){
                sqlStatement += " where (" + IgniteConstants.FROM_COL_NAME;
                sqlStatement += " = '" + fields.get(IgniteConstants.FROM_COL_NAME) + "'";                
                if(fields.containsKey(IgniteConstants.FROM_TYPE_COL_NAME)){
                    sqlStatement += " and " + IgniteConstants.FROM_TYPE_COL_NAME;
                    String id_type = fields.get(IgniteConstants.FROM_TYPE_COL_NAME);
                    sqlStatement += " = '" + id_type + "'";
                }
                
                sqlStatement += " OR " + IgniteConstants.TO_COL_NAME;
                sqlStatement += " = '" + fields.get(IgniteConstants.TO_COL_NAME) + "')";
                if(fields.containsKey(IgniteConstants.TO_TYPE_COL_NAME)){
                    sqlStatement += " and " + IgniteConstants.TO_TYPE_COL_NAME;
                    String id_type = fields.get(IgniteConstants.TO_TYPE_COL_NAME);
                    sqlStatement += " = '" + id_type + "'";
                }
            }
            if (fields.containsKey(IgniteConstants.PROPERTY_KEY_COL_NAME)) {
                /*
                 * A query for those edges of a certain vertex that
                 * match a specific label and value of the selected
                 * property.
                 */
                sqlStatement += " and " + IgniteConstants.PROPERTY_KEY_COL_NAME;
                sqlStatement += " = '" + fields.get(IgniteConstants.PROPERTY_KEY_COL_NAME) + "'";

                sqlStatement += " and " + IgniteConstants.PROPERTY_VALUE_COL_NAME;
                sqlStatement += " = '" + fields.get(IgniteConstants.PROPERTY_VALUE_COL_NAME) + "'";

                sqlStatement += " and " + IgniteConstants.LABEL_COL_NAME;
                sqlStatement += " = '" + fields.get(IgniteConstants.LABEL_COL_NAME) + "'";

            }
            if(fields.containsKey(IgniteConstants.LABEL_COL_NAME)){
                sqlStatement += " and " + IgniteConstants.LABEL_COL_NAME;
                String labels = fields.get(IgniteConstants.LABEL_COL_NAME);

                String[] tokens = labels.split(",");
                if (tokens.length == 1) {
                    sqlStatement += " = '" + tokens[0] + "'";
                } else {
                    List<String> inPart = Stream.of(tokens)
                            .map(token -> "'" + token + "'").collect(Collectors.toList());

                    sqlStatement += " IN (" + String.join(",", inPart) + ")";
                }
            }            


        } catch (Exception e) {
            sqlStatement = null;
        }

    }
}
