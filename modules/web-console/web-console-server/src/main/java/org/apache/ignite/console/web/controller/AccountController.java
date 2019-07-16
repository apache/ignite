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
import org.apache.ignite.console.services.AccountsService;
import org.apache.ignite.console.web.model.ChangeUserRequest;
import org.apache.ignite.console.web.model.EmailRequest;
import org.apache.ignite.console.web.model.ResetPasswordRequest;
import org.apache.ignite.console.web.model.SignUpRequest;
import org.apache.ignite.console.web.model.UserResponse;
import org.springframework.http.ResponseEntity;
import org.springframework.security.authentication.AuthenticationManager;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.web.authentication.preauth.PreAuthenticatedAuthenticationToken;
import org.springframework.security.web.servletapi.SecurityContextHolderAwareRequestWrapper;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import static org.apache.ignite.console.common.Utils.isBecomeUsed;
import static org.springframework.http.MediaType.APPLICATION_JSON_VALUE;
import static org.springframework.security.web.authentication.switchuser.SwitchUserFilter.ROLE_PREVIOUS_ADMINISTRATOR;

/**
 * Controller for accounts API.
 */
@RestController
public class AccountController {
    /** Authentication manager. */
    private final AuthenticationManager authMgr;

    /** Accounts service. */
    private final AccountsService accountsSrvc;

    /**
     * @param authMgr Authentication manager.
     * @param accountsSrvc Accounts service.
     */
    public AccountController(AuthenticationManager authMgr, AccountsService accountsSrvc) {
        this.authMgr = authMgr;
        this.accountsSrvc = accountsSrvc;
    }

    /**
     * @param req Request wrapper.
     */
    @ApiOperation(value = "Get current user.")
    @GetMapping(path = "/api/v1/user")
    public ResponseEntity<UserResponse> user(SecurityContextHolderAwareRequestWrapper req) {
        Account acc = accountsSrvc.loadUserByUsername(req.getUserPrincipal().getName());

        return ResponseEntity.ok(new UserResponse(acc, req.isUserInRole(ROLE_PREVIOUS_ADMINISTRATOR)));
    }

    /**
     * @param params SignUp params.
     */
    @ApiOperation(value = "Register user.")
    @PostMapping(path = "/api/v1/signup")
    public ResponseEntity<Void> signup(@Valid @RequestBody SignUpRequest params) {
        accountsSrvc.register(params);

        Authentication authentication = authMgr.authenticate(
            new UsernamePasswordAuthenticationToken(
                params.getEmail(),
                params.getPassword())
        );

        SecurityContextHolder.getContext().setAuthentication(authentication);

        return ResponseEntity.ok().build();
    }

    /**
     * Save and auth user.
     *
     * @param accId Account id.
     * @param changes Changes to apply to user.
     */
    public Account saveAndAuth(UUID accId, ChangeUserRequest changes) {
        Account acc = accountsSrvc.save(accId, changes);

        Authentication authentication = new PreAuthenticatedAuthenticationToken(
            acc,
            acc.getPassword(),
            acc.getAuthorities()
        );

        SecurityContextHolder.getContext().setAuthentication(authentication);

        return acc;
    }

    /**
     * @param req Request wrapper.
     * @param acc Current user.
     * @param changes Changes to apply to user.
     */
    @ApiOperation(value = "Save user.")
    @PostMapping(path = "/api/v1/profile/save", consumes = APPLICATION_JSON_VALUE)
    public ResponseEntity<UserResponse> save(
        SecurityContextHolderAwareRequestWrapper req,
        @AuthenticationPrincipal Account acc,
        @Valid @RequestBody ChangeUserRequest changes
    ) {
        Account newAcc = saveAndAuth(acc.getId(), changes);

        return ResponseEntity.ok(new UserResponse(newAcc, isBecomeUsed(req)));
    }

    /**
     * @param req Forgot password request.
     */
    @ApiOperation(value = "Send password reset token.")
    @PostMapping(path = "/api/v1/password/forgot", consumes = APPLICATION_JSON_VALUE)
    public ResponseEntity forgotPassword(@Valid @RequestBody EmailRequest req) {
        accountsSrvc.forgotPassword(req.getEmail());

        return ResponseEntity.ok().build();
    }

    /**
     * @param req Reset password request.
     */
    @ApiOperation(value = "Reset user password.")
    @PostMapping(path = "/api/v1/password/reset")
    public ResponseEntity resetPassword(@Valid @RequestBody ResetPasswordRequest req) {
        accountsSrvc.resetPasswordByToken(req.getEmail(), req.getToken(), req.getPassword());

        return ResponseEntity.ok().build();
    }

    /**
     * @param req Forgot password request.
     */
    @ApiOperation(value = "Resend activation token.")
    @PostMapping(path = "/api/v1/activation/resend", consumes = APPLICATION_JSON_VALUE)
    public ResponseEntity activationResend(@Valid @RequestBody EmailRequest req) {
        accountsSrvc.resetActivationToken(req.getEmail());

        return ResponseEntity.ok().build();
    }
}
