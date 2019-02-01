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

package org.apache.ignite.internal.processors.query.h2.opt.join;

import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Context for distributed joins.
 */
public class DistributedJoinContext {
    /** Local flag. */
    private final boolean loc;

    // TODO
    public DistributedJoinContext(boolean loc) {
        this.loc = loc;
    }

    /**
     * @return Local flag.
     */
    public boolean local() {
        return loc;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(DistributedJoinContext.class, this);
    }
}
