/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.query.schema.management;

/**
 * Local database view object.
 */
public class ViewDescriptor {
    /** View name. */
    private final String name;

    /** View SQL. */
    private final String sql;

    /** View description. */
    private final String desc;

    /**
     * @param name View name.
     * @param sql View SQL.
     * @param desc View description.
     */
    ViewDescriptor(String name, String sql, String desc) {
        this.name = name;
        this.sql = sql;
        this.desc = desc;
    }

    /** */
    public String name() {
        return name;
    }

    /** */
    public String sql() {
        return sql;
    }

    /** */
    public String description() {
        return desc;
    }
}
