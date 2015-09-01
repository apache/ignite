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

package org.apache.ignite.internal.visor.debug;

import java.io.Serializable;
import java.lang.management.LockInfo;

/**
 * Data transfer object for {@link LockInfo}.
 */
public class VisorThreadLockInfo implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /**
     * Fully qualified name of the class of the lock object.
     */
    protected final String clsName;

    /**
     * Identity hash code of the lock object.
     */
    protected final Integer identityHashCode;

    /** Create thread lock info with given parameters. */
    public VisorThreadLockInfo(String clsName, Integer identityHashCode) {
        assert clsName != null;

        this.clsName = clsName;
        this.identityHashCode = identityHashCode;
    }

    /** Create data transfer object for given lock info. */
    public static VisorThreadLockInfo from(LockInfo li) {
        assert li != null;

        return new VisorThreadLockInfo(li.getClassName(), li.getIdentityHashCode());
    }

    /**
     * @return Fully qualified name of the class of the lock object.
     */
    public String className() {
        return clsName;
    }

    /**
     * @return Identity hash code of the lock object.
     */
    public Integer identityHashCode() {
        return identityHashCode;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return clsName + '@' + Integer.toHexString(identityHashCode);
    }
}