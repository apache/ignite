/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.internal.util.gridify;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import org.apache.ignite.IgniteException;
import org.apache.ignite.compute.ComputeJobAdapter;
import org.apache.ignite.compute.gridify.GridifyArgument;

/**
 * Convenience adapter for custom {@code gridify} jobs. In addition to
 * functionality provided in {@link org.apache.ignite.compute.ComputeJobAdapter} adapter, this adapter
 * provides default implementation of {@link #execute()} method,
 * which reflectively executes grid-enabled method based on information provided
 * in {@link org.apache.ignite.compute.gridify.GridifyArgument} parameter.
 * <p>
 * Note this adapter is only useful when passing {@link org.apache.ignite.compute.gridify.GridifyArgument} to
 * remote jobs. In many cases, remote jobs will not require {@link org.apache.ignite.compute.gridify.GridifyArgument}
 * as they will execute their code without reflection, hence the regular
 * {@link org.apache.ignite.compute.ComputeJobAdapter} should be used.
 * <p>
 * See {@link org.apache.ignite.compute.gridify.Gridify} documentation for more information about execution of
 * {@code gridified} methods.
 * @see org.apache.ignite.compute.gridify.Gridify
 */
public class GridifyJobAdapter extends ComputeJobAdapter {
    /** */
    private static final long serialVersionUID = 0L;

    /**
     * Initializes job with argument.
     *
     * @param arg Job argument.
     */
    public GridifyJobAdapter(GridifyArgument arg) {
        super(arg);
    }

    /**
     * Provides default implementation for execution of grid-enabled methods.
     * This method assumes that argument passed in is of {@link GridifyArgument}
     * type. It attempts to reflectively execute a method based on information
     * provided in the argument and returns the return value of the method.
     * <p>
     * If some exception occurred during execution, then it will be thrown
     * out of this method.
     *
     * @return {@inheritDoc}
     */
    @Override public Object execute() {
        GridifyArgument arg = argument(0);

        try {
            // Get public, package, protected, or private method.
            Method mtd = arg.getMethodClass().getDeclaredMethod(arg.getMethodName(), arg.getMethodParameterTypes());

            // Attempt to soften access control in case we grid-enabling
            // non-accessible method. Subject to security manager setting.
            if (!mtd.isAccessible())
                try {
                    mtd.setAccessible(true);
                }
                catch (SecurityException e) {
                    throw new IgniteException("Got security exception when attempting to soften access control for " +
                        "@Gridify method: " + mtd, e);
                }

            Object obj = null;

            // No need to create an instance for static methods.
            if (!Modifier.isStatic(mtd.getModifiers()))
                // Obtain instance to execute method on.
                obj = arg.getTarget();

            return mtd.invoke(obj, arg.getMethodParameters());
        }
        catch (InvocationTargetException e) {
            if (e.getTargetException() instanceof IgniteException)
                throw (IgniteException)e.getTargetException();

            throw new IgniteException("Failed to invoke a method due to user exception.", e.getTargetException());
        }
        catch (IllegalAccessException e) {
            throw new IgniteException("Failed to access method for execution.", e);
        }
        catch (NoSuchMethodException e) {
            throw new IgniteException("Failed to find method for execution.", e);
        }
    }
}