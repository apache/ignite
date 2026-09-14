

package org.apache.ignite.console.web.controller;

import java.util.UUID;
import io.swagger.v3.oas.annotations.Operation;
import io.vertx.core.json.JsonArray;

import jakarta.validation.Valid;
import org.apache.ignite.console.dto.Account;
import org.apache.ignite.console.dto.Announcement;

import org.apache.ignite.console.messages.WebConsoleMessageSource;
import org.apache.ignite.console.services.AdminService;
import org.apache.ignite.console.web.model.PeriodFilterRequest;
import org.apache.ignite.console.web.model.SignUpRequest;
import org.apache.ignite.console.web.model.ToggleRequest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.support.MessageSourceAccessor;
import org.springframework.http.ResponseEntity;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;


import static org.springframework.http.MediaType.APPLICATION_JSON_VALUE;

/**
 * Controller for admin API.
 */
@RestController
@RequestMapping("/api/v1/admin")
public class AdminController {
	private static final Logger log = LoggerFactory.getLogger(AdminController.class);
    /** */
    private final AdminService adminSrv;

    /** Messages accessor. */
    private final MessageSourceAccessor messages = WebConsoleMessageSource.getAccessor();

    /**
     * @param adminSrv Admin service.
     */
    public AdminController(AdminService adminSrv) {
        this.adminSrv = adminSrv;
    }

    /**
     * @param period Period filter.
     * @return List of accounts.
     */
    @Operation(summary = "Get a list of users.")
    @PostMapping(path = "/list", consumes = APPLICATION_JSON_VALUE)
    public ResponseEntity<JsonArray> loadAccounts(@io.swagger.v3.oas.annotations.parameters.RequestBody @RequestBody PeriodFilterRequest period) {
        return ResponseEntity.ok(adminSrv.list(period.getStartDate(), period.getEndDate()));
    }

    
    /**
     * @param period Period filter.
     * @return List of accounts.
     */
    @Operation(summary = "Get a user.")
    @GetMapping(path = "/users/{accountId}", consumes = APPLICATION_JSON_VALUE)
    public ResponseEntity<Account> findByIdAccount(@PathVariable("accountId") UUID accId) {
        return ResponseEntity.ok(adminSrv.getUser(accId));
    }
    
    /**
     * @param period Period filter.
     * @return List of accounts.
     */
    @Operation(summary = "Get a user.")
    @GetMapping(path = "/users", consumes = APPLICATION_JSON_VALUE)
    public ResponseEntity<Account> findAccount(@RequestParam("email") String email, @RequestParam("phoneNumber") String phoneNumber) {
        return ResponseEntity.ok(adminSrv.findUser(email,phoneNumber));
    }
    
    /**
     * @param acc Account.
     * @param params Parameters.
     */
    @Operation(summary = "Toggle admin permissions.")
    @PostMapping(path = "/toggle", consumes = APPLICATION_JSON_VALUE)
    public ResponseEntity<Void> toggle(
        @AuthenticationPrincipal Account acc,
        @RequestBody ToggleRequest params
    ) {
        UUID accId = params.getId();
        boolean admin = params.isAdmin();

        if (acc.getId().equals(accId) && !admin)
            throw new IllegalStateException(messages.getMessage("err.prohibited-revoke-admin-rights"));

        adminSrv.toggle(accId, admin);

        return ResponseEntity.ok().build();
    }

    /**
     * @param params SignUp params.
     */
    @Operation(summary = "Register user.")
    @PutMapping(path = "/users")
    public ResponseEntity<Void> registerUser(@Valid @RequestBody SignUpRequest params) {
        adminSrv.registerUser(params);

        return ResponseEntity.ok().build();
    }

    /**
     * @param accId Account ID.
     */
    @Operation(summary = "Delete user.")
    @DeleteMapping(path = "/users/{accountId}")
    public ResponseEntity<Void> delete(@PathVariable("accountId") UUID accId) {
        adminSrv.delete(accId);

        return ResponseEntity.ok().build();
    }

    /**
     * @param ann Update announcement to be shown for all users.
     */
    @Operation(summary = "Update announcement for all connected users.")
    @PutMapping(path = "/announcement", consumes = APPLICATION_JSON_VALUE)
    public ResponseEntity<Void> updateAnnouncement(@RequestBody Announcement ann) {
        adminSrv.updateAnnouncement(ann);

        return ResponseEntity.ok().build();
    }
}
