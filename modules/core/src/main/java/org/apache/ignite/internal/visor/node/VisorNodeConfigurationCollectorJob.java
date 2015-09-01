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

import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.visor.VisorJob;

/**
 * Grid configuration data collect job.
 */
public class VisorNodeConfigurationCollectorJob extends VisorJob<Void, VisorGridConfiguration> {
    /** */
    private static final long serialVersionUID = 0L;

    /**
     * @param arg Formal job argument.
     * @param debug Debug flag.
     */
    public VisorNodeConfigurationCollectorJob(Void arg, boolean debug) {
        super(arg, debug);
    }

    /** {@inheritDoc} */
    @Override protected VisorGridConfiguration run(Void arg) {
        return new VisorGridConfiguration().from(ignite);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(VisorNodeConfigurationCollectorJob.class, this);
    }
}