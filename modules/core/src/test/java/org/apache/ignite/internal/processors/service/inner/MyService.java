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

package org.apache.ignite.internal.processors.service.inner;

import org.apache.ignite.services.Service;

/**
 * Service.
 */
public interface MyService extends Service {
    /** Custom hash code. */
    public static int HASH = 12345;

    /**
     * @return Some value.
     */
    int hello();

    /**
     * hashCode() method with a dummy argument.
     *
     * @param dummy Argument.
     * @return Hash code.
     */
    int hashCode(Object dummy);
}
