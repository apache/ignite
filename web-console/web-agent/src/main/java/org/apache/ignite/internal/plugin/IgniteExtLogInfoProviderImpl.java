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

package org.apache.ignite.internal.plugin;

import java.lang.management.RuntimeMXBean;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.plugin.IgniteLogInfoProviderImpl;
import org.apache.ignite.internal.util.typedef.internal.U;

import static org.apache.ignite.internal.IgniteKernal.NL;
import static org.apache.ignite.internal.IgniteVersionUtils.ACK_VER_STR;
import static org.apache.ignite.internal.IgniteVersionUtils.COPYRIGHT;

/**
 * Change the message output for metrics log.
 */
public class IgniteExtLogInfoProviderImpl extends IgniteLogInfoProviderImpl {
    /** {@inheritDoc} */
    @Override void ackAsciiLogo(IgniteLogger log, IgniteConfiguration cfg, RuntimeMXBean rtBean) {
        
        super.ackAsciiLogo(log,cfg,rtBean);
        
        U.quietAndInfo(log,
            NL + NL +               
                ">>> IgniteHome: " + cfg.getIgniteHome() + NL +                
                ">>> IgniteConsistentId: " + cfg.getConsistentId() + NL +
                ">>> MXBeanName: " + rtBean.getName() + NL +
                ">>> " + NL
        );
    }
}
