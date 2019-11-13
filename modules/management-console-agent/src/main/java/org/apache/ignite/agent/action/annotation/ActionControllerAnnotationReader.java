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

package org.apache.ignite.agent.action.annotation;

import java.lang.reflect.Method;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.ignite.agent.action.ActionMethod;
import org.apache.ignite.internal.util.typedef.F;
import org.reflections.Reflections;

/**
 * Reader for {@link ActionController} annotation.
 */
public class ActionControllerAnnotationReader {
    /** Methods. */
    private static final Map<String, ActionMethod> methods = Collections.unmodifiableMap(
        findActionMethods("org.apache.ignite.agent.action.controller", "org.gridgain.agent.action.controller")
    );

    /**
     * @return Action controllers methods.
     */
    public static Map<String, ActionMethod> actions() {
        return methods;
    }

    /**
     * Find the action methods by specific package.
     *
     * @param basePkgs Base packages.
     * @return Action controllers methods.
     */
    static Map<String, ActionMethod> findActionMethods(String... basePkgs) {
        Map<String, ActionMethod> methods = new HashMap<>();

        Reflections reflections = new Reflections(basePkgs);

        for (Class<?> controllerCls : reflections.getTypesAnnotatedWith(ActionController.class)) {
            ActionController annotation = controllerCls.getAnnotation(ActionController.class);

            String controllerName = F.isEmpty(annotation.value()) ? controllerCls.getSimpleName() : annotation.value();

            for (Method method : controllerCls.getDeclaredMethods()) {
                if (method.isSynthetic())
                    continue;

                String actName = controllerName + '.' + method.getName();

                ActionMethod actMtd = new ActionMethod(actName, method, controllerCls);

                methods.put(actMtd.actionName(), actMtd);
            }
        }

        return methods;
    }
}
