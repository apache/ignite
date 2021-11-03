/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.query.calcite.sql;

/**
 * Enumerates the Ignite specific options for CREATE TABLE statement.
 */
public enum IgniteSqlCreateTableOptionEnum {
    /** A name of the required cache template. */
    TEMPLATE,

    /** A number of partition backups. */
    BACKUPS,

    /** A name of the desired affinity key column. */
    AFFINITY_KEY,

    /** An atomicity mode for the underlying cache. */
    ATOMICITY,

    /** A write synchronization mode for the underlying cache. */
    WRITE_SYNCHRONIZATION_MODE,

    /** A name the group the underlying cache belongs to. */
    CACHE_GROUP,

    /** A name of the underlying cache created by the command. */
    CACHE_NAME,

    /** A name of the data region where table entries should be stored. */
    DATA_REGION,

    /** A name of the custom key type that is used from the key-value and other non-SQL APIs in Ignite. */
    KEY_TYPE,

    /** A name of the custom value type that is used from the key-value and other non-SQL APIs in Ignite. */
    VALUE_TYPE,

    /** This flag specified whether the encryption should be enabled for the underlying cache. */
    ENCRYPTED,
}
