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

package org.apache.ignite.events;

import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.lang.IgniteExperimental;

/**
 *
 */
@IgniteExperimental
public class CacheObjectTransformedEvent extends EventAdapter {
    /** */
    private static final long serialVersionUID = 0L;

    /** Original cache object bytes. */
    private final byte[] original;

    /** Transformed cache object bytes. */
    private final byte[] transformed;

    /** Restore operation. */
    private final boolean restore;

    /**
     * @param node        Node.
     * @param msg         Message.
     * @param type        Type.
     * @param original    Original cache object bytes.
     * @param transformed Transformed cache object bytes.
     * @param restore     Restore operation flag.
     */
    public CacheObjectTransformedEvent(
        ClusterNode node,
        String msg,
        int type,
        byte[] original,
        byte[] transformed,
        boolean restore
    ) {
        super(node, msg, type);

        assert original != null;
        assert transformed != null;

        this.original = original.clone();
        this.transformed = transformed.clone();
        this.restore = restore;
    }

    /**
     * @return Original bytes.
     */
    public byte[] getOriginal() {
        return original;
    }

    /**
     * @return Transformed bytes.
     */
    public byte[] getTransformed() {
        return transformed;
    }

    /**
     * @return True at restore operation.
     */
    public boolean isRestore() {
        return restore;
    }
}
