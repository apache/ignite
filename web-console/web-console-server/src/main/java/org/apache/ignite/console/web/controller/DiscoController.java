

package org.apache.ignite.console.web.controller;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.apache.ignite.Ignite;
import org.apache.ignite.console.dto.Account;
import org.apache.ignite.console.dto.Activity;
import org.apache.ignite.console.repositories.NodeRepository;
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
    private final NodeRepository activitiesSrv;
    
    private Ignite ignite;



    /**
     * @param accountsSrv Accounts server.
     * @param activitiesSrv Administration server.
     */
    public DiscoController(Ignite ignite, AccountsService accountsSrv, NodeRepository activitiesSrv) {
        this.accountsSrv = accountsSrv;
        this.activitiesSrv = activitiesSrv;
        this.ignite = ignite;

    }

    /**
     * @param json SignUp params.
     */
    @Operation(summary = "Register node info with address.")
    @PutMapping(path = "/{group}/{nodeId}/{action}")
    public ResponseEntity<Void> registerActivity(@AuthenticationPrincipal Account acc,
    		@PathVariable("group") String group, @PathVariable("nodeId") UUID nodeId, @PathVariable("action") String action,@RequestBody String json) {

        Activity activity = activitiesSrv.save(acc.getId(), nodeId, group, action, json);

        return ResponseEntity.ok().build();
    }
    
    /**
     * @param action act params.
     */
    @Operation(summary = "Get Register node info with addresses.")
    @GetMapping(path = "/{group}/{action}")
    public ResponseEntity<String> listActivity(@AuthenticationPrincipal Account acc,
    		@PathVariable("group") String group,@PathVariable("action") String action) {
        StringBuilder addresses = new StringBuilder();
        Collection<Activity> list = activitiesSrv.list(acc.getId(), group, action);
        for(Activity act: list) {
        	if(act.json()==null || act.json().isBlank()) {
        		continue;
        	}
        	if(!addresses.isEmpty()) {
        		addresses.append("\n");
        	}
        	addresses.append(act.json());
        }
        if(group.equals(ignite.name())){ // admin instance
            JsonObject st = new JsonObject();
        	TcpDiscoveryNode node = (TcpDiscoveryNode)ignite.configuration().getDiscoverySpi().getLocalNode();
            if(!addresses.isEmpty()) {
                addresses.append("\n");
            }
            JsonArray discoveryAddresses = new JsonArray();
	        Collection<String> adminAddress = node.addresses();
	        for(String host: adminAddress) {
                discoveryAddresses.add(host+"#"+node.discoveryPort());
	        }
            st.put("discoveryAddress",discoveryAddresses);

            node.attributes().forEach((k,v)->{
                if(k.toLowerCase().contains("port")){
                    st.put(k,v);
                }
                else if(k.toLowerCase().contains("host")){
                    st.put(k,v);
                }
            });

            addresses.append(st);
        }

        return ResponseEntity.ok(addresses.toString());
    }

    /**
     * @param acc Account.
     */
    @Operation(summary = "Delete node address.")
    @DeleteMapping(path = "/{group}/{nodeId}")
    public ResponseEntity<Void> deleteActivity(@AuthenticationPrincipal Account acc,
    		@PathVariable("group") String group, @PathVariable("nodeId") UUID nodeId) {
        activitiesSrv.delete(acc.getId(), nodeId);
        return ResponseEntity.ok().build();
    }

    @Operation(summary = "UnRegister Activity status ByAddress")
    @PutMapping(path = "/{group}/{action}/to/node-left")
    public ResponseEntity<Integer> changeActivityByAddress(@AuthenticationPrincipal Account acc,
            @PathVariable("group") String group, @PathVariable("action") String action,@RequestBody String json) {

        JsonArray addresses = new JsonArray(json);
        Collection<Activity> list = activitiesSrv.list(acc.getId(), group, action);
        int c = 0;
        for(Activity act: list) {
            if(act.json()==null || act.json().isBlank()) {
                continue;
            }
            AtomicInteger n = new AtomicInteger();
            JsonObject data = new JsonObject(act.json());
            JsonArray host = data.getJsonArray("discoveryAddress");
            addresses.forEach(a->{
                if(host.remove(a)) {
                    n.incrementAndGet();
                }
            });
            if(n.get()>0) {
                activitiesSrv.save(act.getAccountId(), act.getId(), group, "node-left", data.toString());
            }
        }
        return ResponseEntity.ok(c);
    }
}
