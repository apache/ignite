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

package org.apache.ignite.internal.network.serialization.marshal;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

/**
 * Tests for {@link FlaggedObjectIds}.
 */
class FlaggedObjectIdsTest {
    private static final int OBJECT_ID = 42;

    @Test
    void freshObjectIdIsNotConsideredAsAlreadySeen() {
        long flaggedObjectId = FlaggedObjectIds.freshObjectId(OBJECT_ID);

        assertFalse(FlaggedObjectIds.isAlreadySeen(flaggedObjectId));
    }

    @Test
    void freshObjectIdContainsGivenObjectId() {
        long flaggedObjectId = FlaggedObjectIds.freshObjectId(OBJECT_ID);

        assertThat(FlaggedObjectIds.objectId(flaggedObjectId), is(OBJECT_ID));
    }

    @Test
    void alreadySeenObjectIdIsConsideredAsAlreadySeen() {
        long flaggedObjectId = FlaggedObjectIds.alreadySeenObjectId(OBJECT_ID);

        assertTrue(FlaggedObjectIds.isAlreadySeen(flaggedObjectId));
    }

    @Test
    void alreadySeenObjectIdContainsGivenObjectId() {
        long flaggedObjectId = FlaggedObjectIds.alreadySeenObjectId(OBJECT_ID);

        assertThat(FlaggedObjectIds.objectId(flaggedObjectId), is(OBJECT_ID));
    }

    @Test
    void negativeObjectIdIsExtractedCorrectly() {
        long flaggedObjectId = FlaggedObjectIds.freshObjectId(-1);

        assertThat(FlaggedObjectIds.objectId(flaggedObjectId), is(-1));
    }

    @Test
    void freshnessIsDeterminedCorrectlyWhenObjectIdIsNegative() {
        long flaggedObjectId = FlaggedObjectIds.freshObjectId(-1);

        assertFalse(FlaggedObjectIds.isAlreadySeen(flaggedObjectId));
    }
}
