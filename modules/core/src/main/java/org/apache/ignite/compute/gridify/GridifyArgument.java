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

package org.apache.ignite.compute.gridify;

import java.io.Serializable;

/**
 * Gridify task argument created by the system for task execution. It contains
 * all information needed to reflectively execute a method remotely.
 * <p>
 * Use {@link org.apache.ignite.compute.gridify.aop.GridifyArgumentAdapter} convenience adapter for creating gridify
 * arguments when implementing custom gridify jobs.
 * <p>
 * See {@link Gridify} documentation for more information about execution of
 * {@code gridified} methods.
 * @see Gridify
 */
public interface GridifyArgument extends Serializable {
    /**
     * Gets class to which the executed method belongs.
     *
     * @return Class to which method belongs.
     */
    public Class<?> getMethodClass();

    /**
     * Gets method name.
     *
     * @return Method name.
     */
    public String getMethodName();

    /**
     * Gets method parameter types in the same order they appear in method
     * signature.
     *
     * @return Method parameter types.
     */
    public Class<?>[] getMethodParameterTypes();

    /**
     * Gets method parameters in the same order they appear in method
     * signature.
     *
     * @return Method parameters.
     */
    public Object[] getMethodParameters();

    /**
     * Gets target object to execute method on. {@code Null} for static methods.
     *
     * @return Execution state (possibly {@code null}), required for remote
     *      object creation.
     */
    public Object getTarget();
}