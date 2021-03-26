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

import java.util.UUID;
import io.swagger.annotations.ApiOperation;
import javax.validation.Valid;
import org.apache.ignite.console.dto.Account;
import org.apache.ignite.console.dto.Announcement;
import org.apache.ignite.console.json.JsonArray;
import org.apache.ignite.console.messages.WebConsoleMessageSource;
import org.apache.ignite.console.services.AdminService;
import org.apache.ignite.console.web.model.PeriodFilterRequest;
import org.apache.ignite.console.web.model.SignUpRequest;
import org.apache.ignite.console.web.model.ToggleRequest;
import org.springframework.context.support.MessageSourceAccessor;
import org.springframework.http.ResponseEntity;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import springfox.documentation.annotations.ApiIgnore;

import static org.springframework.http.MediaType.APPLICATION_JSON_VALUE;

/**
 * Controller for admin API.
 */
@RestController
@RequestMapping("/api/v1/admin")
public class AdminController {
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
    @ApiOperation(value = "Get a list of users.")
    @PostMapping(path = "/list", consumes = APPLICATION_JSON_VALUE)
    public ResponseEntity<JsonArray> loadAccounts(@ApiIgnore @RequestBody PeriodFilterRequest period) {
        return ResponseEntity.ok(adminSrv.list(period.getStartDate(), period.getEndDate()));
    }

    /**
     * @param acc Account.
     * @param params Parameters.
     */
    @ApiOperation(value = "Toggle admin permissions.")
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
    @ApiOperation(value = "Register user.")
    @PutMapping(path = "/users")
    public ResponseEntity<Void> registerUser(@Valid @RequestBody SignUpRequest params) {
        adminSrv.registerUser(params);

        return ResponseEntity.ok().build();
    }

    /**
     * @param accId Account ID.
     */
    @ApiOperation(value = "Delete user.")
    @DeleteMapping(path = "/users/{accountId}")
    public ResponseEntity<Void> delete(@PathVariable("accountId") UUID accId) {
        adminSrv.delete(accId);

        return ResponseEntity.ok().build();
    }

    /**
     * @param ann Update announcement to be shown for all users.
     */
    @ApiOperation(value = "Update announcement for all connected users.")
    @PutMapping(path = "/announcement", consumes = APPLICATION_JSON_VALUE)
    public ResponseEntity<Void> updateAnnouncement(@RequestBody Announcement ann) {
        adminSrv.updateAnnouncement(ann);

        return ResponseEntity.ok().build();
    }
}
