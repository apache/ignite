/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.commandline;

import org.apache.ignite.internal.commandline.argument.CommandArg;

/**
 * Transaction command arguments name.
 */
public enum TxCommandArg implements CommandArg {
    /** */
    TX_LIMIT("--limit"),

    /** */
    TX_ORDER("--order"),

    /** */
    TX_SERVERS("--servers"),

    /** */
    TX_CLIENTS("--clients"),

    /** */
    TX_DURATION("--min-duration"),

    /** */
    TX_SIZE("--min-size"),

    /** */
    TX_LABEL("--label"),

    /** */
    TX_NODES("--nodes"),

    /** */
    TX_XID("--xid"),

    /** */
    TX_KILL("--kill"),

    /** */
    TX_INFO("--info");

    /** Option name. */
    private final String name;

    /**
     * @param name Argument name.
     */
    TxCommandArg(String name) {
        this.name = name;
    }

    /** {@inheritDoc} */
    @Override public String argName() {
        return name;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return name;
    }
}
