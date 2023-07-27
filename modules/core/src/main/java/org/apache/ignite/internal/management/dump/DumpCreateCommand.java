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

package org.apache.ignite.internal.management.dump;

import org.apache.ignite.internal.management.api.ComputeCommand;

/**
 *
 */
public class DumpCreateCommand implements ComputeCommand<DumpCreateCommandArg, Void> {
    /** {@inheritDoc} */
    @Override public String description() {
        return "Create a dump of cache groups. Dump is entry by entry consistent copy of cache";
    }

    /** {@inheritDoc} */
    @Override public Class<DumpCreateCommandArg> argClass() {
        return DumpCreateCommandArg.class;
    }

    /** {@inheritDoc} */
    @Override public Class<DumpCreateTask> taskClass() {
        return DumpCreateTask.class;
    }
}
