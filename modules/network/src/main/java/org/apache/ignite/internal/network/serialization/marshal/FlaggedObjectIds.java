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

/**
 * Tools to pack a 32-bit objectId and some flags in a single long (and extract them later).
 * Such representation of a FlaggedObjectId using primitive long is chosen in a pursue of lower memory allocation rate.
 */
class FlaggedObjectIds {
    private static final long LOWER_32_BITS_MASK = 0xFFFFFFFFL;

    static long freshObjectId(int objectId) {
        return flaggedObjectId(objectId, false);
    }

    static long alreadySeenObjectId(int objectId) {
        return flaggedObjectId(objectId, true);
    }

    private static long flaggedObjectId(int objectId, boolean isAlreadySeen) {
        long longFlags = isAlreadySeen ? 1 : 0;
        return (objectId & LOWER_32_BITS_MASK) | (longFlags << 32);
    }

    static int objectId(long flaggedObjectId) {
        return (int) (flaggedObjectId & LOWER_32_BITS_MASK);
    }

    static boolean isAlreadySeen(long flaggedObjectId) {
        int flags = (int) (flaggedObjectId >> 32);

        assert flags == 0 || flags == 1 : flags;

        return flags != 0;
    }
}
