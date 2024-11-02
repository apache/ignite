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

package org.apache.ignite.marshaller;

import java.util.Objects;
import org.apache.ignite.internal.ClassSet;
import org.apache.ignite.lang.IgnitePredicate;

/** */
public class IgniteMarshallerClassFilter implements IgnitePredicate<String> {
    /** */
    private final ClassSet whiteList;

    /** */
    private final ClassSet blackList;

    /**
     * @param whiteList Class white list.
     * @param blackList Class black list.
     */
    public IgniteMarshallerClassFilter(ClassSet whiteList, ClassSet blackList) {
        this.whiteList = whiteList;
        this.blackList = blackList;
    }

    /** {@inheritDoc} */
    @Override public boolean apply(String s) {
        // Allows all primitive arrays and checks arrays' type.
        if ((blackList != null || whiteList != null) && s.charAt(0) == '[') {
            if (s.charAt(1) == 'L' && s.length() > 2)
                s = s.substring(2, s.length() - 1);
            else
                return true;
        }

        return (blackList == null || !blackList.contains(s)) && (whiteList == null || whiteList.contains(s));
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (!(o instanceof IgniteMarshallerClassFilter))
            return false;

        IgniteMarshallerClassFilter other = (IgniteMarshallerClassFilter)o;

        return Objects.equals(whiteList, other.whiteList) && Objects.equals(blackList, other.blackList);
    }
}
