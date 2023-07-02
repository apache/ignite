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

import de.kp.works.ignite.IgniteColumn;
import de.kp.works.ignite.IgniteConstants;

import java.util.ArrayList;
import java.util.List;

public class IgniteResult {
    /**
     * [IgniteResult] represents all cache entries
     * that refer to a certain edge, row, etc.
     */
    public IgniteResult() {}

    private List<IgniteColumn> columns = new ArrayList<>();

    public void addColumn(String colName, String colType, Object colValue) {
    	if(!colName.equals("*")) {
    		columns.add(new IgniteColumn(colName, colType, colValue));
    	}
    }

    /**
     * This method returns the user identifier assigned
     * to edges, vertices and other
     */
    public Object getId() {
        return getValue(IgniteConstants.ID_COL_NAME);
    }

    public Object getValue(String key) {

        Object value = null;
        for (IgniteColumn column : columns) {
            if (column.getColName().equals(key)) {
                value = column.getColValue();
            }
        }

        return value;

    }

    public List<IgniteColumn> getColumns() {
        return columns;
    }

    public boolean isEmpty() {
         return columns.isEmpty();
    }
}
