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

package org.apache.ignite.internal.processors.query.h2.sql;

/**
 * AST for SQL.
 */
public interface GridSqlAst {
    /**
     * @return Generate sql from this AST.
     */
    public String getSQL();

    /**
     * @return Number of child nodes.
     */
    public int size();

    /**
     * Get child by index.
     *
     * @param childIdx Index of the requested child.
     * @return Child element.
     */
    public <E extends GridSqlAst> E child(int childIdx);

    /**
     * Get the first child.
     *
     * @return Child element.
     */
    public <E extends GridSqlAst> E child();

    /**
     * Set child.
     *
     * @param childIdx Index of the requested child.
     * @param child Child element.
     */
    public <E extends GridSqlAst> void child(int childIdx, E child);

    /**
     * @return Optional expression result type (if this is an expression and result type is known).
     */
    public GridSqlType resultType();
}
