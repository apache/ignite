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

package org.apache.ignite.mxbean;

import org.apache.ignite.spi.systemview.view.ServiceView;

/**
 * Service MXBean interface.
 *
 * @see org.apache.ignite.internal.management.api.CommandMBean
 * @deprecated Use managements API beans, instead.
 */
@Deprecated
public interface ServiceMXBean {
    /**
     * @param name Service name.
     * @see ServiceView#name()
     */
    @MXBeanDescription("Kills service by the name.")
    public void cancel(
        @MXBeanParameter(name = "name", description = "Service name.") String name
    );
}
