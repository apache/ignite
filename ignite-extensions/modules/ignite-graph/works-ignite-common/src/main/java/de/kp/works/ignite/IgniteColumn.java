package de.kp.works.ignite;
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

public class IgniteColumn {

    private final String colName;
    private String colType;
    private Object colValue;

    public IgniteColumn(String colName, String colType, Object colValue) {
        this.colName = colName;
        this.colType = colType;

        this.colValue = colValue;
    }

    public IgniteColumn(String colName) {
        this.colName = colName;
    }
    public IgniteColumn(String colName, String colType) {
        this.colName = colName;
        this.colType = colType;
    }

    public String getColName() {
        return colName;
    }

    public String getColType() {
        return colType;
    }

    public Object getColValue() {
        return colValue;
    }
}
