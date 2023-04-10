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

package org.apache.ignite.internal.management;

import lombok.Data;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.internal.management.api.ConfirmableCommand;
import org.apache.ignite.internal.management.api.EnumDescription;
import org.apache.ignite.internal.management.api.Parameter;
import org.apache.ignite.internal.management.api.PositionalParameter;

/**
 *
 */
@Data
public class SetStateCommand extends ConfirmableCommand {
    /** */
    @PositionalParameter()
    @EnumDescription(
        names = {
            "ACTIVE",
            "INACTIVE",
            "ACTIVE_READ_ONLY"
        },
        descriptions = {
            "Activate cluster. Cache updates are allowed",
            "Deactivate cluster",
            "Activate cluster. Cache updates are denied"
        }
    )
    private ClusterState state;

    /** */
    @Parameter(optional = true, excludeFromDescription = true)
    private Boolean force;

    /** {@inheritDoc} */
    @Override public String description() {
        return "Change cluster state";
    }
}
