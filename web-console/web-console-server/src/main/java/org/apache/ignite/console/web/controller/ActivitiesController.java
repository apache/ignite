

package org.apache.ignite.console.web.controller;

import io.swagger.v3.oas.annotations.Operation;
import org.apache.ignite.console.dto.Account;
import org.apache.ignite.console.services.ActivitiesService;
import org.apache.ignite.console.web.model.ActivityRequest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import static org.springframework.http.MediaType.APPLICATION_JSON_VALUE;

/**
 * Controller for activities API.
 */
@RestController
@RequestMapping(path = "/api/v1/activities")
public class ActivitiesController {
    /** */
    private final ActivitiesService activitiesSrv;

    /**
     * @param activitiesSrv Activities service.
     */
    @Autowired
    public ActivitiesController(ActivitiesService activitiesSrv) {
        this.activitiesSrv = activitiesSrv;
    }

    /**
     * @param acc Account.
     */
    @Operation(summary = "Save user's activity.")
    @PostMapping(path = "/page", consumes = APPLICATION_JSON_VALUE)
    public ResponseEntity<Void> save(@AuthenticationPrincipal Account acc, @RequestBody ActivityRequest req) {
        activitiesSrv.save(acc.getId(), req.getGroup(), req.getAction());

        return ResponseEntity.ok().build();
    }
}
