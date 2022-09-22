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

public class IgniteLimitQuery extends IgniteQuery {

    private String queryType;
    /**
     * Retrieves a specified number of (ordered) elements
     * from the beginning of the cache. Note, this query
     * is restricted to elements with a numeric identifier.
     */
    public IgniteLimitQuery(String cacheName, IgniteAdmin admin, int limit) {
        super(cacheName, admin);
        /*
         * Transform the provided properties into fields
         */
        HashMap<String, String> fields = new HashMap<>();
        fields.put(IgniteConstants.LIMIT_VALUE, String.valueOf(limit));

        queryType = "withId";
        createSql(fields);
    }

    public IgniteLimitQuery(String cacheName, IgniteAdmin admin, Object fromId, int limit) {
        super(cacheName, admin);
        /*
         * Transform the provided properties into fields
         */
        HashMap<String, String> fields = new HashMap<>();

        fields.put(IgniteConstants.FROM_COL_NAME, fromId.toString());
        fields.put(IgniteConstants.LIMIT_VALUE, String.valueOf(limit));

        queryType = "withFrom";
        createSql(fields);
    }

    public IgniteLimitQuery(String cacheName, IgniteAdmin admin,
                            String label, String key, Object inclusiveFrom, int limit, boolean reversed) {
        super(cacheName, admin);
        /*
         * Transform the provided properties into fields
         */
        HashMap<String, String> fields = new HashMap<>();

        fields.put(IgniteConstants.LABEL_COL_NAME, label);
        fields.put(IgniteConstants.PROPERTY_KEY_COL_NAME, key);

        fields.put(IgniteConstants.INCLUSIVE_FROM_VALUE, inclusiveFrom.toString());
        fields.put(IgniteConstants.LIMIT_VALUE, String.valueOf(limit));

        fields.put(IgniteConstants.REVERSED_VALUE, String.valueOf(reversed));

        queryType = "withProp";
        createSql(fields);
    }

    @Override
    protected void createSql(Map<String, String> fields) {
        try {
            buildSelectPart();

            if (queryType.equals("withId")) {
                sqlStatement += " limit " + fields.get(IgniteConstants.LIMIT_VALUE);

            }
            else if (queryType.equals("withFrom")) {
                /*
                 * Build the `clause` of the SQL statement
                 * from the provided fields
                 */
                sqlStatement += " where " + IgniteConstants.FROM_COL_NAME;
                sqlStatement += " = '" + fields.get(IgniteConstants.FROM_COL_NAME) + "'";

                sqlStatement += " limit " + fields.get(IgniteConstants.LIMIT_VALUE);

            }
            else {
                /*
                 * Build the `clause` of the SQL statement
                 * from the provided fields
                 */
                sqlStatement += " where " + IgniteConstants.LABEL_COL_NAME;
                sqlStatement += " = '" + fields.get(IgniteConstants.LABEL_COL_NAME) + "'";

                sqlStatement += " and " + IgniteConstants.PROPERTY_KEY_COL_NAME;
                sqlStatement += " = '" + fields.get(IgniteConstants.PROPERTY_KEY_COL_NAME) + "'";
                /*
                 * The value of the value column must in the range of
                 * INCLUSIVE_FROM_VALUE >= PROPERTY_VALUE_COL_NAME
                 */
                sqlStatement += " and " + IgniteConstants.PROPERTY_VALUE_COL_NAME;
                sqlStatement += " >= '" + fields.get(IgniteConstants.INCLUSIVE_FROM_VALUE) + "'";
                /*
                 * Determine sorting order
                 */
                if (fields.get(IgniteConstants.REVERSED_VALUE).equals("true")) {
                    sqlStatement += " order by " + IgniteConstants.PROPERTY_VALUE_COL_NAME + " DESC";
                } else {
                    sqlStatement += " order by " + IgniteConstants.PROPERTY_VALUE_COL_NAME + " ASC";
                }

                sqlStatement += " limit " + fields.get(IgniteConstants.LIMIT_VALUE);

            }
        } catch (Exception e) {
            sqlStatement = null;
        }

    }
}
