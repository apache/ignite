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

package org.apache.ignite.hadoop.util;

import org.jetbrains.annotations.Nullable;

import java.io.Serializable;

/**
 * Hadoop file system name mapper. Ensures that correct user name is passed to the underlying Hadoop file system.
 */
public interface UserNameMapper extends Serializable {
    /**
     * Map user name.
     *
     * @param name User name.
     * @return Mapped user name.
     */
    @Nullable public String map(String name);
}
