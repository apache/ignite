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

package org.apache.ignite.internal.visor.node;

import java.io.Serializable;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.visor.util.VisorTaskUtils.compactArray;

/**
 * Data transfer object for node P2P configuration properties.
 */
public class VisorPeerToPeerConfiguration implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Whether peer-to-peer class loading is enabled. */
    private boolean p2pEnabled;

    /** Missed resource cache size. */
    private int p2pMissedResCacheSize;

    /** List of packages from the system classpath that need to be loaded from task originating node. */
    private String p2pLocClsPathExcl;

    /**
     * @param c Grid configuration.
     * @return Data transfer object for node P2P configuration properties.
     */
    public static VisorPeerToPeerConfiguration from(IgniteConfiguration c) {
        VisorPeerToPeerConfiguration cfg = new VisorPeerToPeerConfiguration();

        cfg.p2pEnabled = c.isPeerClassLoadingEnabled();
        cfg.p2pMissedResCacheSize = c.getPeerClassLoadingMissedResourcesCacheSize();
        cfg.p2pLocClsPathExcl = compactArray(c.getPeerClassLoadingLocalClassPathExclude());

        return cfg;
    }

    /**
     * @return Whether peer-to-peer class loading is enabled.
     */
    public boolean p2pEnabled() {
        return p2pEnabled;
    }

    /**
     * @return Missed resource cache size.
     */
    public int p2pMissedResponseCacheSize() {
        return p2pMissedResCacheSize;
    }

    /**
     * @return List of packages from the system classpath that need to be loaded from task originating node.
     */
    @Nullable public String p2pLocalClassPathExclude() {
        return p2pLocClsPathExcl;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(VisorPeerToPeerConfiguration.class, this);
    }
}