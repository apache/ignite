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

import io.swagger.v3.oas.annotations.Operation;
import org.apache.ignite.console.dto.Account;
import org.apache.ignite.console.services.DemoService;
import org.springframework.http.ResponseEntity;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * Controller for demo API.
 */
@RestController
@RequestMapping(path = "/api/v1/demo")
public class DemoController {
    /** */
    private DemoService demoSrv;

    /**
     * @param demoSrv Demo service.
     */
    public DemoController(DemoService demoSrv) {
        this.demoSrv = demoSrv;
    }

    /**
     * @param acc Account.
     */
    @Operation(summary = "Reset demo configuration.")
    @PostMapping(path = "/reset")
    public ResponseEntity<Void> reset(@AuthenticationPrincipal Account acc) {
        demoSrv.reset(acc.getId());

        return ResponseEntity.ok().build();
    }
}
