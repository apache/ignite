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

public class IgniteEdgesExistQuery extends IgniteQuery {

    public IgniteEdgesExistQuery(String cacheName, IgniteAdmin admin,
                            Object vertex) {
        super(cacheName, admin);

        HashMap<String, String> fields = new HashMap<>();

        fields.put(IgniteConstants.TO_COL_NAME, vertex.toString());
        fields.put(IgniteConstants.FROM_COL_NAME, vertex.toString());

        createSql(fields);

    }

    @Override
    protected void createSql(Map<String, String> fields) {
        try {
            buildSelectPart();
            /*
             * Build `where` clause
             */
            sqlStatement += " where " + IgniteConstants.TO_COL_NAME;
            sqlStatement += " = '" + fields.get(IgniteConstants.TO_COL_NAME) + "'";

            sqlStatement += " or " + IgniteConstants.FROM_COL_NAME;
            sqlStatement += " = '" + fields.get(IgniteConstants.FROM_COL_NAME) + "'";

        } catch (Exception e) {
            sqlStatement = null;
        }

    }

}
