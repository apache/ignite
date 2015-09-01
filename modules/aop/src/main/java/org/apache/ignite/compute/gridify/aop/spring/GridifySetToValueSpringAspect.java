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

package org.apache.ignite.compute.gridify.aop.spring;

import java.lang.reflect.Method;
import org.aopalliance.intercept.MethodInterceptor;
import org.aopalliance.intercept.MethodInvocation;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.compute.gridify.GridifyInterceptor;
import org.apache.ignite.compute.gridify.GridifyNodeFilter;
import org.apache.ignite.compute.gridify.GridifyRuntimeException;
import org.apache.ignite.compute.gridify.GridifySetToValue;
import org.apache.ignite.compute.gridify.aop.GridifySetToValueAbstractAspect;
import org.apache.ignite.internal.util.gridify.GridifyArgumentBuilder;
import org.apache.ignite.internal.util.gridify.GridifyRangeArgument;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.G;

import static org.apache.ignite.IgniteState.STARTED;
import static org.apache.ignite.internal.util.gridify.GridifyUtils.UNKNOWN_SIZE;

/**
 * Spring aspect that cross-cuts on all methods grid-enabled with
 * {@link GridifySetToValue} annotation and potentially executes them on
 * remote node.
 * <p>
 * Note that Spring uses proxy-based AOP, so in order to be properly
 * cross-cut, all methods need to be enhanced with {@link GridifySpringEnhancer}
 * helper.
 * <p>
 * See {@link GridifySetToValue} documentation for more information about execution of
 * {@code gridified} methods.
 * @see GridifySetToValue
 */
public class GridifySetToValueSpringAspect extends GridifySetToValueAbstractAspect implements MethodInterceptor {
    /**
     * Aspect implementation which executes grid-enabled methods on remote
     * nodes.
     *
     * {@inheritDoc}
     */
    @SuppressWarnings({"ProhibitedExceptionDeclared", "ProhibitedExceptionThrown", "CatchGenericClass"})
    @Override public Object invoke(MethodInvocation invoc) throws Throwable {
        Method mtd = invoc.getMethod();

        GridifySetToValue ann = mtd.getAnnotation(GridifySetToValue.class);

        assert ann != null : "Intercepted method does not have gridify annotation.";

        // Since annotations in Java don't allow 'null' as default value
        // we have accept an empty string and convert it here.
        // NOTE: there's unintended behavior when user specifies an empty
        // string as intended grid name.
        // NOTE: the 'ann.gridName() == null' check is added to mitigate
        // annotation bugs in some scripting languages (e.g. Groovy).
        String gridName = F.isEmpty(ann.gridName()) ? null : ann.gridName();

        if (G.state(gridName) != STARTED)
            throw new IgniteCheckedException("Grid is not locally started: " + gridName);

        GridifyNodeFilter nodeFilter = null;

        if (!ann.nodeFilter().equals(GridifyNodeFilter.class))
            nodeFilter = ann.nodeFilter().newInstance();

        GridifyArgumentBuilder argBuilder = new GridifyArgumentBuilder();

        // Creates task argument.
        GridifyRangeArgument arg = argBuilder.createTaskArgument(
            mtd.getDeclaringClass(),
            mtd.getName(),
            mtd.getReturnType(),
            mtd.getParameterTypes(),
            mtd.getParameterAnnotations(),
            invoc.getArguments(),
            invoc.getThis());

        if (!ann.interceptor().equals(GridifyInterceptor.class)) {
            // Check interceptor first.
            if (!ann.interceptor().newInstance().isGridify(ann, arg))
                return invoc.proceed();
        }

        // Proceed locally for negative threshold parameter.
        if (ann.threshold() < 0)
            return invoc.proceed();

        // Analyse where to execute method (remotely or locally).
        if (arg.getInputSize() != UNKNOWN_SIZE && arg.getInputSize() <= ann.threshold())
            return invoc.proceed();

        // Check is split to jobs allowed for input method argument with declared splitSize.
        checkIsSplitToJobsAllowed(arg, ann);

        try {
            Ignite ignite = G.ignite(gridName);

            return execute(mtd, ignite.compute(), invoc.getMethod().getDeclaringClass(), arg, nodeFilter,
                ann.threshold(), ann.splitSize(), ann.timeout());
        }
        catch (Exception e) {
            for (Class<?> ex : invoc.getMethod().getExceptionTypes()) {
                // Descend all levels down.
                Throwable cause = e.getCause();

                while (cause != null) {
                    if (ex.isAssignableFrom(cause.getClass()))
                        throw cause;

                    cause = cause.getCause();
                }

                if (ex.isAssignableFrom(e.getClass()))
                    throw e;
            }

            throw new GridifyRuntimeException("Undeclared exception thrown: " + e.getMessage(), e);
        }
    }
}