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

package org.apache.ignite.cache;

import org.apache.ignite.*;
import org.apache.ignite.internal.util.typedef.*;
import org.jetbrains.annotations.*;

import java.util.*;

/**
 * Exception thrown when projection flags check fails.
 */
public class GridCacheFlagException extends IgniteException {
    /** */
    private static final long serialVersionUID = 0L;

    /** Flags that caused this exception. */
    private Collection<GridCacheFlag> flags;

    /**
     * @param flags Cause flags.
     */
    public GridCacheFlagException(@Nullable GridCacheFlag... flags) {
        this(F.asList(flags));
    }

    /**
     * @param flags Cause flags.
     */
    public GridCacheFlagException(@Nullable Collection<GridCacheFlag> flags) {
        super(message(flags));

        this.flags = flags;
    }

    /**
     * @return Cause flags.
     */
    public Collection<GridCacheFlag> flags() {
        return flags;
    }

    /**
     * @param flags Flags.
     * @return String information about cause flags.
     */
    private static String message(Collection<GridCacheFlag> flags) {
        return "Cache projection flag violation (if flag is LOCAL, make sure to use peek(..) " +
            "instead of get(..) methods)" + (F.isEmpty(flags) ? "." : " [flags=" + flags + ']');
    }
}
