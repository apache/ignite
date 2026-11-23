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
package collectors;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.IOVersions;
import framework.BuiltTree;
import framework.Collector;
import framework.Declarations;

/**
 * Describes the latest version of each type of persisted page. Ignite reads a page by the type and version stored in it, and the
 * versions of a type go from 1 to the latest. So a new version changes what old nodes can read, and removing one makes old data
 * unreadable. A type is identified by its stored number: the page classes are internal. Versions that Ignite registers when a
 * node starts, such as those of H2 index pages, are not described.
 */
public class PageVersions implements Collector {
    /** {@inheritDoc} */
    @Override public void describe(BuiltTree tree, Declarations out) throws Exception {
        int found = 0;

        for (Class<?> cls : tree.classes("", false, out)) {
            for (Field f : cls.getDeclaredFields()) {
                if (!Modifier.isStatic(f.getModifiers()) || f.getType() != IOVersions.class)
                    continue;

                IOVersions<?> vers = (IOVersions<?>)Declarations.staticValue(f);

                if (vers == null)
                    continue;

                out.add("Stored data", "latest version of page type " + vers.getType(), String.valueOf(vers.latest().getVersion()), "",
                    tree.where(cls, f.getName(), -1, false));

                found++;
            }
        }

        if (found == 0)
            out.error("No page versions found");
    }
}
