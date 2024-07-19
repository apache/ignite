package de.kp.works.ignite.gremlin;
/*
 * Copyright (c) 2019 - 2021 Dr. Krusche & Partner PartG. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 *
 * @author Stefan Krusche, Dr. Krusche & Partner PartG
 *
 */

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.kp.works.ignite.IgniteAdmin;
import de.kp.works.ignite.IgniteConf;
import de.kp.works.ignite.IgniteConnect;

/**
 * [IgniteConnection] is used in the IgniteGraph context
 * and provides [IgniteAdmin] as the user interface
 */
public class IgniteConnection {
	private static final Logger LOGGER = LoggerFactory.getLogger(IgniteConnection.class);    
    /*
     * [IgniteAdmin] is an API access class that hides
     * the functionality of the [Ignite] client
     */
    private IgniteAdmin admin;

    public IgniteConnection(String namespace,String cfg) {
    	try {
            
    		IgniteConnect connect = IgniteConnect.getInstance(new IgniteConf(cfg,namespace));
            admin = new IgniteAdmin(connect);

        } catch (Exception e) {
            String message = "Connecting to Apache Ignited failed";
            LOGGER.error(message, e);
        }
    	
    }

    public IgniteAdmin getAdmin() {
        return admin;
    }
    
    public String namespace() {
        return admin.namespace();
    }
}
