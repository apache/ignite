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

package org.apache.ignite.internal.processors.security.impl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;

import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.security.AbstractSecurityPluginProvider;
import org.apache.ignite.internal.processors.security.GridSecurityProcessor;
import static org.apache.ignite.plugin.security.SecurityPermission.*;
import org.apache.ignite.plugin.security.SecurityPermissionSet;
import org.apache.ignite.plugin.security.SecurityPermissionSetBuilder;

/** 
 * 基于Mongodb的用户权限系统
 **/
public class MongoSecurityPluginProvider extends AbstractSecurityPluginProvider {
    /** root Login. */
    private  String login;

    /** root Password. */
    private  String pwd;

    /** Permissions. */
    private  SecurityPermissionSet perms;

    /** Users security data. */
    private  Collection<SimpleSecurityData> clientData = new ArrayList<>();
    
    public MongoSecurityPluginProvider() {
          this.login = "root";
          this.pwd = "";          
          
          SecurityPermissionSet permsSet = new SecurityPermissionSetBuilder()        		 
        		            .appendTaskPermissions("*", TASK_CANCEL)
        		            .appendTaskPermissions("*", TASK_EXECUTE)
        		           .appendSystemPermissions(ADMIN_VIEW, EVENTS_ENABLE)
        		           .build();
          
          perms = permsSet;
    }

    /** {@inheritDoc} */
    @Override public String name() {
        return "MongoSecurityProcessorProvider";
    }

    
    /** */
    public MongoSecurityPluginProvider(String login, String pwd, SecurityPermissionSet perms) {
        this.login = login;
        this.pwd = pwd;
        this.perms = perms;
      
    }

    /** {@inheritDoc} */
    @Override protected GridSecurityProcessor securityProcessor(GridKernalContext ctx) {
    	return null;
		/*
		 * return new SimpleSecurityProcessor(ctx, new SimpleSecurityData(login, pwd,
		 * perms), clientData);
		 */
    }
}
