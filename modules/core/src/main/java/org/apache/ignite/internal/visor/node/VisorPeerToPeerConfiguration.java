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

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Arrays;
import java.util.List;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.visor.VisorDataTransferObject;
import org.jetbrains.annotations.Nullable;

/**
 * Data transfer object for node P2P configuration properties.
 */
public class VisorPeerToPeerConfiguration extends VisorDataTransferObject {
    /** */
    private static final long serialVersionUID = 0L;

    /** Whether peer-to-peer class loading is enabled. */
    private boolean p2pEnabled;

    /** Missed resource cache size. */
    private int p2pMissedResCacheSize;

    /** List of packages from the system classpath that need to be loaded from task originating node. */
    private List<String> p2pLocClsPathExcl;

    /**
     * Default constructor.
     */
    public VisorPeerToPeerConfiguration() {
        // No-op.
    }

    /**
     * Create data transfer object for node P2P configuration properties.
     *
     * @param c Grid configuration.
     */
    public VisorPeerToPeerConfiguration(IgniteConfiguration c) {
        p2pEnabled = c.isPeerClassLoadingEnabled();
        p2pMissedResCacheSize = c.getPeerClassLoadingMissedResourcesCacheSize();
        p2pLocClsPathExcl = Arrays.asList(c.getPeerClassLoadingLocalClassPathExclude());
    }

    /**
     * @return Whether peer-to-peer class loading is enabled.
     */
    public boolean isPeerClassLoadingEnabled() {
        return p2pEnabled;
    }

    /**
     * @return Missed resource cache size.
     */
    public int getPeerClassLoadingMissedResourcesCacheSize() {
        return p2pMissedResCacheSize;
    }

    /**
     * @return List of packages from the system classpath that need to be loaded from task originating node.
     */
    @Nullable public List<String> getPeerClassLoadingLocalClassPathExclude() {
        return p2pLocClsPathExcl;
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        out.writeBoolean(p2pEnabled);
        out.writeInt(p2pMissedResCacheSize);
        U.writeCollection(out, p2pLocClsPathExcl);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(byte protoVer, ObjectInput in) throws IOException, ClassNotFoundException {
        p2pEnabled = in.readBoolean();
        p2pMissedResCacheSize = in.readInt();
        p2pLocClsPathExcl = U.readList(in);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(VisorPeerToPeerConfiguration.class, this);
    }
}
