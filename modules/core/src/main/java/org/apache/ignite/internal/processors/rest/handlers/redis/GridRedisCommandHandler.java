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

package org.apache.ignite.internal.processors.rest.handlers.redis;

import java.util.Collection;
import java.util.List;

import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.rest.protocols.tcp.redis.GridRedisCommand;
import org.apache.ignite.internal.processors.rest.protocols.tcp.redis.GridRedisMessage;
import org.apache.ignite.internal.util.nio.GridNioSession;

/**
 * Command handler.
 */
public interface GridRedisCommandHandler {
    /**
     * @return Collection of supported commands.
     */
    public Collection<GridRedisCommand> supportedCommands();

    /**
     * @param ses Session.
     * @param msg Request message.
     * @return Future.
     */
    public IgniteInternalFuture<GridRedisMessage> handleAsync(GridNioSession ses, GridRedisMessage msg);
    
    /**
     * named params
     * @param name
     * @param params
     * @return
     */
    public default String stringValue(String name,List<String> params) {
    	for(int i=0;i<params.size();i++) {
    		if(params.get(i).equalsIgnoreCase(name) && i+1<params.size()) {
    			return params.get(i+1);
    		}
    	}
    	return null;
    }
}
