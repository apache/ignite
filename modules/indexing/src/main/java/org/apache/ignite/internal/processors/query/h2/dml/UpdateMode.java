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

package org.apache.ignite.internal.processors.query.h2.dml;

/**
 * DML statement execution plan type - MERGE/INSERT from rows or subquery,
 * or UPDATE/DELETE from subquery or literals/params based.
 */
public enum UpdateMode {
    /** MERGE command. */
    MERGE,

    /** INSERT command. */
    INSERT,

    /** UPDATE command. */
    UPDATE,

    /** DELETE command. */
    DELETE,

    /** COPY command. */
    BULK_LOAD
}
