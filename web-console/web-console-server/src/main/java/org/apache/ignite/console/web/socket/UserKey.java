/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.console.web.socket;

import java.util.Objects;
import java.util.UUID;

/**
 * User session key.
 */
public class UserKey {
    /** Account ID. */
    private final UUID accId;

    /** Demo flag. */
    private final boolean demo;

    /**
     * @param accId Account ID.
     * @param demo Demo flag.
     */
    public UserKey(UUID accId, boolean demo) {
        this.accId = accId;
        this.demo = demo;
    }

    /**
     * @return value of account ID.
     */
    public UUID getAccId() {
        return accId;
    }

    /**
     * @return value of demo flag.
     */
    public boolean isDemo() {
        return demo;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (o == null || getClass() != o.getClass())
            return false;

        UserKey key = (UserKey)o;

        return demo == key.demo && accId.equals(key.accId);
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return Objects.hash(accId, demo);
    }
}
