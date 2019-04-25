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

package org.apache.ignite.internal.processors.platform.utils;

import org.apache.ignite.*;
import org.apache.ignite.lang.*;

/**
 * Platform listenable.
 */
public interface PlatformListenable {
    /**
     * Listen.
     *
     * @param lsnr Listener.
     */
    public void listen(IgniteBiInClosure<Object, Throwable> lsnr);

    /**
     * Cancel this instance.
     *
     * @return True if canceled.
     */
    public boolean cancel() throws IgniteCheckedException;

    /**
     * Returns true if this listenable was canceled before completion.
     *
     * @return True if this listenable was canceled before completion.
     */
    public boolean isCancelled();
}
