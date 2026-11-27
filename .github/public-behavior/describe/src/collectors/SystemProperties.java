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
import java.util.Collection;
import org.apache.ignite.SystemProperty;
import org.apache.ignite.startup.cmdline.CommandLineStartup;
import framework.BuiltTree;
import framework.Collector;
import framework.Declarations;

/** Describes the system properties that {@code ignite.sh -systemProps} prints, without their descriptions. */
public class SystemProperties implements Collector {
    /** Field of {@link CommandLineStartup} that lists the classes declaring the system properties. */
    private static final String PROPS_CLS = "PROPS_CLS";

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public void describe(BuiltTree tree, Declarations out) throws Exception {
        Field propsCls = CommandLineStartup.class.getDeclaredField(PROPS_CLS);

        propsCls.setAccessible(true);

        for (Class<?> cls : (Collection<Class<?>>)propsCls.get(null)) {
            for (Field f : cls.getFields()) {
                SystemProperty prop = f.getAnnotation(SystemProperty.class);

                if (prop == null)
                    continue;

                boolean deprecated = f.isAnnotationPresent(Deprecated.class);
                String dflt = prop.type() == Boolean.class && prop.defaults().isEmpty() ? "false" : prop.defaults();

                out.add("Configuration", "system property " + f.get(null), prop.type().getSimpleName()
                    + (dflt.isEmpty() ? "" : ", default " + dflt) + (deprecated ? ", deprecated" : ""),
                    deprecated ? Declarations.DEPRECATED : "", tree.where(f.getDeclaringClass(), f.getName()));
            }
        }
    }
}
