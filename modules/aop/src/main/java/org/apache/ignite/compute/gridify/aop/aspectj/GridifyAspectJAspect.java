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

package org.apache.ignite.compute.gridify.aop.aspectj;

import java.lang.reflect.Method;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.compute.ComputeTask;
import org.apache.ignite.compute.gridify.Gridify;
import org.apache.ignite.compute.gridify.GridifyArgument;
import org.apache.ignite.compute.gridify.GridifyInterceptor;
import org.apache.ignite.compute.gridify.GridifyRuntimeException;
import org.apache.ignite.compute.gridify.aop.GridifyArgumentAdapter;
import org.apache.ignite.compute.gridify.aop.GridifyDefaultTask;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.G;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.reflect.MethodSignature;

import static org.apache.ignite.IgniteState.STARTED;

/**
 * AspectJ aspect that cross-cuts on all methods grid-enabled with
 * {@link org.apache.ignite.compute.gridify.Gridify} annotation and potentially executes them on
 * remote node.
 * <p>
 * See {@link org.apache.ignite.compute.gridify.Gridify} documentation for more information about execution of
 * {@code gridified} methods.
 * @see org.apache.ignite.compute.gridify.Gridify
 */
@Aspect
public class GridifyAspectJAspect {
    /**
     * Aspect implementation which executes grid-enabled methods on remote
     * nodes.
     *
     * @param joinPnt Join point provided by AspectJ AOP.
     * @return Method execution result.
     * @throws Throwable If execution failed.
     */
    @SuppressWarnings({"ProhibitedExceptionDeclared", "ProhibitedExceptionThrown", "CatchGenericClass", "unchecked"})
    @Around("execution(@org.apache.ignite.compute.gridify.Gridify * *(..)) && !cflow(call(* org.apache.ignite.compute.ComputeJob.*(..)))")
    public Object gridify(ProceedingJoinPoint joinPnt) throws Throwable {
        Method mtd = ((MethodSignature) joinPnt.getSignature()).getMethod();

        Gridify ann = mtd.getAnnotation(Gridify.class);

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

        // Initialize defaults.
        GridifyArgument arg = new GridifyArgumentAdapter(mtd.getDeclaringClass(), mtd.getName(),
                mtd.getParameterTypes(), joinPnt.getArgs(), joinPnt.getTarget());

        if (!ann.interceptor().equals(GridifyInterceptor.class)) {
            // Check interceptor first.
            if (!ann.interceptor().newInstance().isGridify(ann, arg))
                return joinPnt.proceed();
        }

        if (!ann.taskClass().equals(GridifyDefaultTask.class) && !ann.taskName().isEmpty()) {
            throw new IgniteCheckedException("Gridify annotation must specify either Gridify.taskName() or " +
                "Gridify.taskClass(), but not both: " + ann);
        }

        try {
            Ignite ignite = G.ignite(gridName);

            // If task class was specified.
            if (!ann.taskClass().equals(GridifyDefaultTask.class)) {
                return ignite.compute().withTimeout(ann.timeout()).execute(
                    (Class<? extends ComputeTask<GridifyArgument, Object>>)ann.taskClass(), arg);
            }

            // If task name was not specified.
            if (ann.taskName().isEmpty()) {
                return ignite.compute().withTimeout(ann.timeout()).execute(new GridifyDefaultTask(
                    joinPnt.getSignature().getDeclaringType()), arg);
            }

            // If task name was specified.
            return ignite.compute().withTimeout(ann.timeout()).execute(ann.taskName(), arg);
        }
        catch (Exception e) {
            for (Class<?> ex : ((MethodSignature) joinPnt.getSignature()).getMethod().getExceptionTypes()) {
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