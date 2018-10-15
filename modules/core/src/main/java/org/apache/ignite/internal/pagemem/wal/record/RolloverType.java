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

package org.apache.ignite.internal.pagemem.wal.record;

/**
 * Defines WAL logging type with regard to segment rollover.
 */
public enum RolloverType {
    /** Record being logged is not a rollover record. */
    NONE,

    /**
     * Record being logged is a rollover record and it should get to the current segment whenever possible.
     * If current segment is full, then the record gets to the next segment. Anyway, logging implementation should
     * guarantee segment rollover afterwards.
     */
    CURRENT_SEGMENT,

    /**
     * Record being logged is a rollover record and it should become the first record in the next segment.
     */
    NEXT_SEGMENT;
}
