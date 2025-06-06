package org.apache.ignite.console.web.controller;

import java.util.Collection;
import java.util.UUID;
import io.swagger.v3.oas.annotations.Operation;
import lombok.Synchronized;

import org.apache.ignite.console.dto.Account;
import org.apache.ignite.console.dto.TaskFlow;
import org.apache.ignite.console.repositories.TaskFlowRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.web.bind.annotation.*;

import static org.springframework.http.MediaType.APPLICATION_JSON_VALUE;

/**
 * Controller for taskFlows API.
 */
@RestController
@RequestMapping(path = "/api/v1/taskflow")
public class TaskFlowController {
    /** */
    private final TaskFlowRepository taskFlowsSrv;

    /**
     * @param taskFlowsSrv Notebooks service.
     */
    @Autowired
    public TaskFlowController(TaskFlowRepository taskFlowsSrv) {
        this.taskFlowsSrv = taskFlowsSrv;
    }
    
    /**
     * @param acc Account.
     * @param flowId Notebook ID.
     */
    @Operation(summary = "get user's taskflow.")
    @GetMapping(path = "/{taskflowId}")
    public ResponseEntity<TaskFlow> get(
        @AuthenticationPrincipal Account acc,
        @PathVariable("taskflowId") UUID taskflowId
    ) {
    	TaskFlow dto = taskFlowsSrv.get(acc.getId(), taskflowId);
    	
        return ResponseEntity.ok(dto);
    }

    /**
     * @param acc Account.
     * @return Collection of taskFlows.
     */
    @Operation(summary = "Get user's grouped taskFlows.")
    @GetMapping(path = "/group/{groupId}")
    public ResponseEntity<Collection<TaskFlow>> list(@AuthenticationPrincipal Account acc,
    		@PathVariable("groupId") String groupId,@RequestParam("sourceCluster") String sourceCluster, @RequestParam(value = "target", required = false) String target, @RequestParam(value = "source",required = false) String source) {
        return ResponseEntity.ok(taskFlowsSrv.taskFlowForGroup(acc.getId(),groupId, sourceCluster, target, source));
    }
    
    /**
     * @param acc Account.
     * @return Collection of taskFlows.
     */
    @Operation(summary = "Get user's grouped taskFlows.")
    @GetMapping(path = "/cluster/{clusterId}")
    public ResponseEntity<Collection<TaskFlow>> listOfCache(@AuthenticationPrincipal Account acc,
                                                            @PathVariable("clusterId") String clusterId,
                                                            @RequestParam(value = "action",required = false) String action,
                                                            @RequestParam("target") String target) {
        return ResponseEntity.ok(taskFlowsSrv.taskFlowForCache(acc.getId(),clusterId, action, target));
    }
    

    /**
     * @param acc Account.
     */
    @Operation(summary = "Save user's flow.")
    @PutMapping(consumes = APPLICATION_JSON_VALUE)
    public synchronized ResponseEntity<?> save(@AuthenticationPrincipal Account acc, @RequestBody TaskFlow flow) {
    	if(flow.getTarget()==null || flow.getSource()==null) {
    		return ResponseEntity.badRequest().body("TargetCache or SourceCache must not null!");
    	}
    	Collection<TaskFlow> exists = taskFlowsSrv.taskFlowForGroup(acc.getId(),flow.getGroup(),flow.getSourceCluster(), flow.getTarget(), flow.getSource());
        if(flow.getId()==null) {
        	if(exists.size()>0) {
        		flow.setId(exists.iterator().next().getId());
        		flow.setAccountId(acc.getId());
        	}
        	else {
        		flow.setAccountId(acc.getId());
        		flow.setId(UUID.randomUUID());
        	}
        	
        }
        else if(!exists.isEmpty()) {
        	UUID existId = exists.iterator().next().getId();
        	if(!flow.getId().equals(existId)) {
        		return ResponseEntity.badRequest().body("TaskFlow "+ existId+" already existing!");
        	}
        }
        if(flow.getAccountId()==null) {        	
        	flow.setAccountId(acc.getId());
        }
    	taskFlowsSrv.save(acc.getId(), flow);

        return ResponseEntity.ok().body(flow.getId());
    }
    
    /**
     * @param acc Account.
     */
    @Operation(summary = "Save user's flow. must have flow id")
    @PutMapping(value="/advance", consumes = APPLICATION_JSON_VALUE)
    public ResponseEntity<?> saveAdvance(@AuthenticationPrincipal Account acc, @RequestBody TaskFlow flow) {
        if(flow.getId()==null) {
        	return ResponseEntity.badRequest().body("TaskFlow.id must not null!");
        }
        if(flow.getAccountId()==null) {        	
        	flow.setAccountId(acc.getId());
        }
    	taskFlowsSrv.save(acc.getId(), flow);

        return ResponseEntity.ok().body(flow.getId());
    }
    
    /**
     * @param acc Account.
     * @param flowId Notebook ID.
     */
    @Operation(summary = "Delete user's grouped flow.")
    @DeleteMapping(path = "/group/{groupId}")
    public ResponseEntity<Void> delete(
        @AuthenticationPrincipal Account acc,
        @PathVariable("groupId") String grpId
    ) {
        taskFlowsSrv.delete(acc.getId(), grpId);

        return ResponseEntity.ok().build();
    }

    /**
     * @param acc Account.
     * @param flowId Notebook ID.
     */
    @Operation(summary = "Delete user's flow.")
    @DeleteMapping(path = "/{flowId}")
    public ResponseEntity<Void> delete(
        @AuthenticationPrincipal Account acc,
        @PathVariable("flowId") UUID flowId
    ) {
        taskFlowsSrv.delete(acc.getId(), flowId);

        return ResponseEntity.ok().build();
    }
}
