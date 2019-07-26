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

package org.apache.ignite.tests.p2p;

import java.util.concurrent.ThreadLocalRandom;
import org.apache.ignite.IgniteSet;

/**
 * Represents user defined key for {@link IgniteSet}.
 */
public class IgniteSetCustomKey {
    /** Dummy field. */
    private final int val;

    /**
     * Creates a new instance of custom key and initializes it with a random value.
     */
    public IgniteSetCustomKey() {
        this.val = ThreadLocalRandom.current().nextInt();
    }

    /**
     * @return Dummy value.
     */
    public int value() {
        return val;
    }
}
