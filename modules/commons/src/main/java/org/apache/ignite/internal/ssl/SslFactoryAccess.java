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

package org.apache.ignite.internal.ssl;

import java.util.function.Function;
import org.apache.ignite.ssl.AbstractSslContextFactory;
import org.apache.ignite.ssl.SslBundle;

/** Access to the factory's key material for node status and reload. */
public final class SslFactoryAccess {
    /** */
    private final Function<AbstractSslContextFactory, SslBundle> current;

    /** */
    private final Function<AbstractSslContextFactory, SslBundle> reload;

    /**
     * @param current Reads the accepted material.
     * @param reload Loads and publishes replacement material.
     */
    public SslFactoryAccess(
        Function<AbstractSslContextFactory, SslBundle> current,
        Function<AbstractSslContextFactory, SslBundle> reload
    ) {
        this.current = current;
        this.reload = reload;
    }

    /** @return The factory's accepted material, initialized on first use. */
    SslBundle current(AbstractSslContextFactory factory) {
        return current.apply(factory);
    }

    /** @return Fresh material accepted by the factory. */
    SslBundle reload(AbstractSslContextFactory factory) {
        return reload.apply(factory);
    }
}
