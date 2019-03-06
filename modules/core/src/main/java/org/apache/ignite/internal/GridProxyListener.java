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

package org.apache.ignite.internal;

import java.util.EventListener;

/**
 * Interception listener is notified about method apply. For each intercepted method
 * apply the listener will be called twice - before and after the apply.
 * <p>
 * Method {@link #beforeCall(Class, String, Object[])} is called right before the
 * traceable method and the second apply {@link #afterCall(Class, String, Object[], Object, Throwable)}
 * is made to get invocation result and exception, if there was one.
 */
public interface GridProxyListener extends EventListener {
    /**
     * Method is called right before the traced method.
     *
     * @param cls Callee class.
     * @param mtdName Callee method name.
     * @param args Callee method parameters.
     */
    public void beforeCall(Class<?> cls, String mtdName, Object[] args);

    /**
     * Method is called right after the traced method.
     *
     * @param cls Callee class.
     * @param mtdName Callee method name.
     * @param args Callee method parameters.
     * @param res Call result. Might be {@code null} if apply
     *      returned {@code null} or if exception happened.
     * @param e Exception thrown by given method apply, if any. Can be {@code null}.
     */
    public void afterCall(Class<?> cls, String mtdName, Object[] args, Object res, Throwable e);
}