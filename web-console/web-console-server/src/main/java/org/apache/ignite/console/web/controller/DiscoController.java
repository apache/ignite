/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.console.web.controller;

import java.net.InetSocketAddress;
import java.util.Collection;

import org.apache.ignite.Ignite;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.console.common.Test;
import org.apache.ignite.console.dto.Account;
import org.apache.ignite.console.dto.Activity;
import org.apache.ignite.console.repositories.ActivitiesRepository;
import org.apache.ignite.console.services.AccountsService;
import org.apache.ignite.spi.discovery.tcp.internal.TcpDiscoveryNode;
import org.springframework.http.ResponseEntity;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import io.swagger.v3.oas.annotations.Operation;

/**
 * Controller for discover API.
 */
@RestController
@RequestMapping("/api/v1/disco")
public class DiscoController {
    /** Accounts service. */
    protected AccountsService accountsSrv;
    
    /** Node Activities */
    private final ActivitiesRepository activitiesSrv;
    
    private Ignite ignite;

    /**
     * @param accountsSrv Accounts server.
     * @param adminSrv Administration server.
     */
    public DiscoController(Ignite ignite, AccountsService accountsSrv, ActivitiesRepository activitiesSrv) {
        this.accountsSrv = accountsSrv;
        this.activitiesSrv = activitiesSrv;
        this.ignite = ignite;        
    }

    /**
     * @param params SignUp params.
     */
    @Operation(summary = "Register node address.")
    @PutMapping(path = "/{group}")
    public ResponseEntity<Void> registerActivity(@AuthenticationPrincipal Account acc,
    		@PathVariable("group") String group,@RequestBody String addresses) {
       
        String[] addressList = addresses.split(",");
        for(String address: addressList) {
        	activitiesSrv.save(acc.getId(), group, address);
        }

        return ResponseEntity.ok().build();
    }
    
    /**
     * @param params SignUp params.
     */
    @Operation(summary = "Get Register node addresses.")
    @GetMapping(path = "/{group}")
    public ResponseEntity<String> listActivity(@AuthenticationPrincipal Account acc,
    		@PathVariable("group") String group) {
        StringBuilder addresses = new StringBuilder();
        Collection<Activity> list = activitiesSrv.list(acc.getId(), group);
        for(Activity act: list) {
        	if(addresses.length()>0) {
        		addresses.append(",");
        	}
        	addresses.append(act.getAction());
        }
        if(group.equals(ignite.name())){
	        TcpDiscoveryNode node = (TcpDiscoveryNode)ignite.configuration().getDiscoverySpi().getLocalNode();
	        
	        Collection<String> adminAddress = node.addresses();
	        for(String act: adminAddress) {
	        	if(addresses.length()>0) {
	        		addresses.append(",");
	        	}
	        	addresses.append(act+"#"+node.discoveryPort());
	        }
        }

        return ResponseEntity.ok(addresses.toString());
    }

    /**
     * @param email Account email.
     */
    @Operation(summary = "Delete node address.")
    @DeleteMapping(path = "/{group}")
    public ResponseEntity<Void> deleteActivity(@AuthenticationPrincipal Account acc,
    		@PathVariable("group") String group,@RequestParam("addresses") String addresses) {
        
    	String[] addressList = addresses.split(",");
        for(String address: addressList) {
        	activitiesSrv.delete(acc.getId(), group, address);
        }

        return ResponseEntity.ok().build();
    }
}
