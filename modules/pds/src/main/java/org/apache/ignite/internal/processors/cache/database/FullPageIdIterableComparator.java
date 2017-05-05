/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cache.database;

import java.io.Serializable;
import java.util.Comparator;
import org.apache.ignite.internal.util.typedef.T2;

/**
 *
 */
public class FullPageIdIterableComparator implements Comparator<T2<Integer, Integer>>, Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    public static final FullPageIdIterableComparator INSTANCE = new FullPageIdIterableComparator();

    /** {@inheritDoc} */
    @Override public int compare(T2<Integer, Integer> o1, T2<Integer, Integer> o2) {
        if (o1.get1() < o2.get1())
            return -1;

        if (o1.get1() > o2.get1())
            return 1;

        if (o1.get2() < o2.get2())
            return -1;

        if (o1.get2() > o2.get2())
            return 1;

        return 0;
    }
}

