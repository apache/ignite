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

package org.apache.ignite.internal.classpath;

import java.util.UUID;
import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.util.distributed.DistributedProcess;
import org.apache.ignite.plugin.extensions.communication.Message;

/**
 * Class path deploy to all response for {@link DistributedProcess} initiate message.
 */
public class ClassPathDeployToAllResponse implements Message {

    /** Ignite class path id. */
    @Order(0)
    UUID icpId;

    /** */
    public ClassPathDeployToAllResponse(UUID icpId) {
        this.icpId = icpId;
    }
}
