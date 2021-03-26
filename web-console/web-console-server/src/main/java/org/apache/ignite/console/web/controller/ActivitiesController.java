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

import io.swagger.annotations.ApiOperation;
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
    @ApiOperation(value = "Save user's activity.")
    @PostMapping(path = "/page", consumes = APPLICATION_JSON_VALUE)
    public ResponseEntity<Void> save(@AuthenticationPrincipal Account acc, @RequestBody ActivityRequest req) {
        activitiesSrv.save(acc.getId(), req.getGroup(), req.getAction());

        return ResponseEntity.ok().build();
    }
}
