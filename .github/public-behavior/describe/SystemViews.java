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

import java.lang.reflect.ParameterizedType;
import java.util.Arrays;
import org.apache.ignite.internal.systemview.SystemViewRowAttributeWalkerGenerator;
import org.apache.ignite.spi.systemview.view.SystemViewRowAttributeWalker;

/**
 * Describes system view columns in the order that the walkers generated during the build give them to clients. The class of the
 * view row is a rename group: the SQL name of the view is registered at runtime, and renaming the class changes nothing.
 */
class SystemViews implements Collector {
    /** {@inheritDoc} */
    @Override public void describe(BuiltTree tree, Declarations out) throws Exception {
        int walkers = 0;

        for (Class<?> cls : tree.classes(SystemViewRowAttributeWalkerGenerator.WALKER_PACKAGE, false, out)) {
            if (cls.isInterface() || !SystemViewRowAttributeWalker.class.isAssignableFrom(cls))
                continue;

            Class<?> row = (Class<?>)Arrays.stream(cls.getGenericInterfaces())
                .filter(t -> t instanceof ParameterizedType && ((ParameterizedType)t).getRawType() == SystemViewRowAttributeWalker.class)
                .map(t -> ((ParameterizedType)t).getActualTypeArguments()[0]).findFirst().orElseThrow();

            walkers++;

            ((SystemViewRowAttributeWalker<?>)cls.getDeclaredConstructor().newInstance()).visitAll(
                new SystemViewRowAttributeWalker.AttributeVisitor() {
                    @Override public <T> void accept(int idx, String name, Class<T> type) {
                        out.add("Monitoring", "system view " + row.getSimpleName() + " column " + name,
                            "position " + (idx + 1) + ", " + type.getTypeName(), "", tree.where(row, name, 0, false), null,
                            row.getSimpleName());
                    }
                });
        }

        if (walkers == 0)
            out.error("No system view walkers found in " + SystemViewRowAttributeWalkerGenerator.WALKER_PACKAGE);
    }
}
