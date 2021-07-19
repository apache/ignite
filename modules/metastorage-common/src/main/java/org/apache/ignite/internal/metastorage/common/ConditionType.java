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

package org.apache.ignite.internal.metastorage.common;

/**
 * Defines possible condition types which can be applied to the revision.
 */
public enum ConditionType {
    /** Equality condition type for revision. */
    REV_EQUAL,

    /** Inequality condition type for revision. */
    REV_NOT_EQUAL,

    /** Greater than condition type for revision. */
    REV_GREATER,

    /** Less than condition type for revision. */
    REV_LESS,

    /** Less than or equal to condition type for revision. */
    REV_LESS_OR_EQUAL,

    /** Greater than or equal to condition type for revision. */
    REV_GREATER_OR_EQUAL,

    /** Equality condition type for value. */
    VAL_EQUAL,

    /** Inequality condition type for value. */
    VAL_NOT_EQUAL,

    /** Existence condition type for key. */
    KEY_EXISTS,

    /** Non-existence condition type for key. */
    KEY_NOT_EXISTS,

    /** Tombstone condition type for key. */
    TOMBSTONE
}
