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

package org.apache.ignite.internal.processors.igfs;

import org.apache.ignite.internal.processors.hadoop.HadoopClassLoader;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.thread.IgniteThreadFactory;
import org.jetbrains.annotations.NotNull;

/**
 * Special thread factory used only for IGFS pool which prevents {@link HadoopClassLoader} leak into
 * {@code Thread.contextClassLoader} field. To achieve this we switch context class loader back and forth when
 * creating threads.
 */
public class IgfsThreadFactory extends IgniteThreadFactory {
    /**
     * Constructor.
     *
     * @param igniteInstanceName Ignite instance name.
     * @param threadName Thread name.
     */
    public IgfsThreadFactory(String igniteInstanceName, String threadName) {
        super(igniteInstanceName, threadName);
    }

    /** {@inheritDoc} */
    @Override public Thread newThread(@NotNull Runnable r) {
        Thread curThread = Thread.currentThread();

        ClassLoader oldLdr = curThread.getContextClassLoader();

        curThread.setContextClassLoader(IgfsThreadFactory.class.getClassLoader());

        try {
            return super.newThread(r);
        }
        finally {
            curThread.setContextClassLoader(oldLdr);
        }
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(IgfsThreadFactory.class, this);
    }
}
