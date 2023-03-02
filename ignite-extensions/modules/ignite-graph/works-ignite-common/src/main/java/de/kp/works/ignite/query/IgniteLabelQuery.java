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
import de.kp.works.ignite.graph.ElementType;

import java.util.HashMap;
import java.util.Map;

public class IgniteLabelQuery extends IgniteQuery {
    /**
     * Retrieve all elements that refer to the provided
     * label, either edges or vertices.
     */
    public IgniteLabelQuery(String cacheName, IgniteAdmin admin, String label) {
        super(cacheName, admin);
        /*
         * Transform the provided properties into fields
         */
        HashMap<String, String> fields = new HashMap<>();

        fields.put(IgniteConstants.LABEL_COL_NAME, label);
        createSql(fields);

    }

    @Override
    protected void createSql(Map<String, String> fields) {
        try {
            buildSelectPart();
            /*
             * Build the `clause` of the SQL statement
             * from the provided fields
             */
            if(this.elementType != ElementType.DOCUMENT) {
            	
            	sqlStatement += " where " + IgniteConstants.LABEL_COL_NAME;
                sqlStatement += " = '" + fields.get(IgniteConstants.LABEL_COL_NAME) + "'";
            }
            
        } catch (Exception e) {
            sqlStatement = null;
        }

    }
}
