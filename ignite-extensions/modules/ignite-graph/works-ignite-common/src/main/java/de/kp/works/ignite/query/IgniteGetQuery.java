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
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class IgniteGetQuery extends IgniteQuery {
    /**
     * Retrieve the element (edge or vertex) that refers
     * to the provided identifier
     */
    public IgniteGetQuery(String cacheName, IgniteAdmin admin, Object id) {
        super(cacheName, admin);
        /*
         * Transform the provided properties into fields
         */
        HashMap<String, String> fields = new HashMap<>();

        fields.put(IgniteConstants.ID_COL_NAME, id.toString());
        createSql(fields);

    }
    /**
     * Retrieve all elements (edges or vertices) that refer
     * to the provided list of identifiers
     */
    public IgniteGetQuery(String cacheName, IgniteAdmin admin, List<Object> ids) {
        super(cacheName, admin);
        /*
         * Transform the provided properties into fields
         */
        HashMap<String, String> fields = new HashMap<>();
        fields.put(IgniteConstants.ID_COL_NAME, ids.stream()
                .map(Object::toString).collect(Collectors.joining(",")));

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
            sqlStatement += " where " + IgniteConstants.ID_COL_NAME;
            String ids = fields.get(IgniteConstants.ID_COL_NAME);

            String[] tokens = ids.split(",");
            if (tokens.length == 1) {
                sqlStatement += " = '" + tokens[0] + "'";
            } else {
                List<String> inPart = Stream.of(tokens)
                        .map(token -> "'" + token + "'").collect(Collectors.toList());

                sqlStatement += " in(" + String.join(",", inPart) + ")";
            }

        } catch (Exception e) {
            sqlStatement = null;
        }

    }
}
