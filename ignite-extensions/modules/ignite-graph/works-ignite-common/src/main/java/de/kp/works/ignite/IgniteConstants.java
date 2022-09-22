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

public class IgniteConstants {

    /**
     * Table names
     */
    public static final String EDGES          = "edges";
    public static final String VERTICES       = "vertices";

    /**
     * Internal keys
     */
    public static final String LONG_COL_TYPE    = "LONG";
    public static final String STRING_COL_TYPE  = "STRING";

    public static final String CREATED_AT_COL_NAME = "created_at";
    public static final String UPDATED_AT_COL_NAME = "updated_at";

    public static final String FROM_COL_NAME = "from";
    public static final String FROM_TYPE_COL_NAME = "from_type";
    public static final String TO_COL_NAME = "to";
    public static final String TO_TYPE_COL_NAME = "to_type";

    public static final String LABEL_COL_NAME = "label";

    public static final String ID_COL_NAME   = "id";
    public static final String ID_TYPE_COL_NAME   = "id_type";

    public static final String PROPERTY_KEY_COL_NAME = "property_key";
    public static final String PROPERTY_TYPE_COL_NAME = "property_type";
    public static final String PROPERTY_VALUE_COL_NAME = "property_value";

    public static final String INCLUSIVE_FROM_VALUE = "inclusive_from_value";
    public static final String EXCLUSIVE_TO_VALUE = "inclusive_to_value";

    public static final String LIMIT_VALUE = "limit_value";
    public static final String REVERSED_VALUE = "reversed_value";

}
