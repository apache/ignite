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

package org.apache.ignite.marshaller;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import javax.management.MBeanServer;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.compute.ComputeJobContext;
import org.apache.ignite.compute.ComputeLoadBalancer;
import org.apache.ignite.compute.ComputeTaskContinuousMapper;
import org.apache.ignite.compute.ComputeTaskSession;
import org.apache.ignite.internal.GridComponent;
import org.apache.ignite.internal.GridLoggerProxy;
import org.apache.ignite.internal.executor.GridExecutorService;
import org.apache.ignite.internal.util.GridBoundedConcurrentLinkedHashMap;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * Controls what classes should be excluded from marshalling by default.
 */
public final class MarshallerExclusions {
    /**
     * Classes that must be included in serialization. All marshallers must
     * included these classes.
     * <p>
     * Note that this list supersedes {@link #EXCL_CLASSES}.
     */
    private static final Class<?>[] INCL_CLASSES = new Class[] {
        // Ignite classes.
        GridLoggerProxy.class,
        GridExecutorService.class
    };

    /** */
    private static volatile Map<Class<?>, Boolean> cache = new GridBoundedConcurrentLinkedHashMap<>(
        512, 512, 0.75f, 16);

    /**
     * Excluded grid classes from serialization. All marshallers must omit
     * these classes. Fields of these types should be serialized as {@code null}.
     * <p>
     * Note that {@link #INCL_CLASSES} supersedes this list.
     */
    private static final Class<?>[] EXCL_CLASSES;

    /**
     *
     */
    static {
        Class springCtxCls = null;

        try {
            springCtxCls = Class.forName("org.springframework.context.ApplicationContext");
        }
        catch (Exception ignored) {
            // No-op.
        }

        List<Class<?>> excl = new ArrayList<>();

        // Non-Ignite classes.
        excl.add(MBeanServer.class);
        excl.add(ExecutorService.class);
        excl.add(ClassLoader.class);
        excl.add(Thread.class);

        if (springCtxCls != null)
            excl.add(springCtxCls);

        // Ignite classes.
        excl.add(IgniteLogger.class);
        excl.add(ComputeTaskSession.class);
        excl.add(ComputeLoadBalancer.class);
        excl.add(ComputeJobContext.class);
        excl.add(Marshaller.class);
        excl.add(GridComponent.class);
        excl.add(ComputeTaskContinuousMapper.class);

        EXCL_CLASSES = U.toArray(excl, new Class[excl.size()]);
    }

    /**
     * Ensures singleton.
     */
    private MarshallerExclusions() {
        // No-op.
    }

    /**
     * Checks given class against predefined set of excluded types.
     *
     * @param cls Class to check.
     * @return {@code true} if class should be excluded, {@code false} otherwise.
     */
    @SuppressWarnings("ForLoopReplaceableByForEach")
    private static boolean isExcluded0(Class<?> cls) {
        assert cls != null;

        final Class<?>[] inc = INCL_CLASSES;

        // NOTE: don't use foreach for performance reasons.
        for (int i = 0; i < inc.length; i++)
            if (inc[i].isAssignableFrom(cls))
                return false;

        final Class<?>[] exc = EXCL_CLASSES;

        // NOTE: don't use foreach for performance reasons.
        for (int i = 0; i < exc.length; i++)
            if (exc[i].isAssignableFrom(cls))
                return true;

        return false;
    }

    /**
     * Checks whether or not given class should be excluded from marshalling.
     *
     * @param cls Class to check.
     * @return {@code true} if class should be excluded, {@code false} otherwise.
     */
    public static boolean isExcluded(Class<?> cls) {
        Boolean res = cache.get(cls);

        if (res == null) {
            res = isExcluded0(cls);

            cache.put(cls, res);
        }

        return res;
    }

    /**
     * Intended for test purposes only.
     */
    public static void clearCache() {
        cache = new GridBoundedConcurrentLinkedHashMap<>(512, 512, 0.75f, 16);
    }
}
