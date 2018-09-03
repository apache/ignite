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

package org.apache.ignite.internal.visor.query;

/**
 * Arguments for {@link VisorQueryTask}.
 */
public class VisorQueryArgV3 extends VisorQueryArgV2 {
    /** */
    private static final long serialVersionUID = 0L;

    /** Enforce join order flag. */
    private final boolean enforceJoinOrder;

    /**
     * @param cacheName Cache name for query.
     * @param qryTxt Query text.
     * @param distributedJoins If {@code true} then distributed joins enabled.
     * @param enforceJoinOrder If {@code true} then enforce join order.
     * @param loc Flag whether to execute query locally.
     * @param pageSize Result batch size.
     */
    public VisorQueryArgV3(String cacheName, String qryTxt,
        boolean distributedJoins, boolean enforceJoinOrder, boolean loc, int pageSize) {
        super(cacheName, qryTxt, distributedJoins, loc, pageSize);

        this.enforceJoinOrder = enforceJoinOrder;
    }

    /**
     * @return Enforce join order flag.
     */
    public boolean enforceJoinOrder() {
        return enforceJoinOrder;
    }
}
