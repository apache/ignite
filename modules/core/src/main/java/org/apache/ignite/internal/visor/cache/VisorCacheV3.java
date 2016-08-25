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

package org.apache.ignite.internal.visor.cache;

import java.util.Collection;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.internal.util.lang.GridTuple3;

/**
 * Data transfer object for {@link IgniteCache}.
 *
 * @deprecated Needed only for backward compatibility.
 */
public class VisorCacheV3 extends VisorCacheV2 {
    /** */
    private static final long serialVersionUID = 0L;

    /** @deprecated Needed only for backward compatibility. */
    private Collection<GridTuple3<Integer, Long, Long>> primaryPartsOffheapSwap;

    /** @deprecated Needed only for backward compatibility. */
    private Collection<GridTuple3<Integer, Long, Long>> backupPartsOffheapSwap;

    /**
     * @deprecated Needed only for backward compatibility.
     */
    public Collection<GridTuple3<Integer, Long, Long>> primaryPartitionsOffheapSwap() {
        return primaryPartsOffheapSwap;
    }

    /**
     * @deprecated Needed only for backward compatibility.
     */
    public Collection<GridTuple3<Integer, Long, Long>> backupPartitionsOffheapSwap() {
        return backupPartsOffheapSwap;
    }
}
