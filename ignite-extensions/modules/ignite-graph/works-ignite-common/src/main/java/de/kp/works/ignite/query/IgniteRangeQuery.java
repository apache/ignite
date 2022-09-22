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

public class IgniteRangeQuery extends IgniteQuery {
    /**
     * Retrieves all elements that refer to specific
     * label and a range of values of a property that
     * can be sorted in ASC order.
     */
    public IgniteRangeQuery(String cacheName, IgniteAdmin admin,
                            String label, String key, Object inclusiveFromValue, Object exclusiveToValue) {
        super(cacheName, admin);
        /*
         * Transform the provided properties into fields
         */
        HashMap<String, String> fields = new HashMap<>();

        fields.put(IgniteConstants.LABEL_COL_NAME, label);
        fields.put(IgniteConstants.PROPERTY_KEY_COL_NAME, key);

        fields.put(IgniteConstants.INCLUSIVE_FROM_VALUE, inclusiveFromValue.toString());
        fields.put(IgniteConstants.EXCLUSIVE_TO_VALUE, exclusiveToValue.toString());

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
            sqlStatement += " where " + IgniteConstants.LABEL_COL_NAME;
            sqlStatement += " = '" + fields.get(IgniteConstants.LABEL_COL_NAME) + "'";

            sqlStatement += " and " + IgniteConstants.PROPERTY_KEY_COL_NAME;
            sqlStatement += " = '" + fields.get(IgniteConstants.PROPERTY_KEY_COL_NAME) + "'";
            /*
             * The value of the value column must in the range of
             * INCLUSIVE_FROM_VALUE >= PROPERTY_VALUE_COL_NAME < EXCLUSIVE_TO_VALUE
             */
            sqlStatement += " and " + IgniteConstants.PROPERTY_VALUE_COL_NAME;
            sqlStatement += " >= '" + fields.get(IgniteConstants.INCLUSIVE_FROM_VALUE) + "'";

            sqlStatement += " and " + IgniteConstants.PROPERTY_VALUE_COL_NAME;
            sqlStatement += " < '" + fields.get(IgniteConstants.EXCLUSIVE_TO_VALUE) + "'";

            sqlStatement += " order by " + IgniteConstants.PROPERTY_VALUE_COL_NAME + " ASC";

        } catch (Exception e) {
            sqlStatement = null;
        }

    }
}
