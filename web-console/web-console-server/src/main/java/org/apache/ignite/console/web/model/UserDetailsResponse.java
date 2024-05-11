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

package org.apache.ignite.console.web.model;

import java.util.UUID;
import io.swagger.v3.oas.annotations.media.Schema;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.NotEmpty;

/**
 * Web model of the user.
 */
public class UserDetailsResponse extends UserResponse {
    /** ID */
    @Schema(title = "User ID.")
    @NotNull
    @NotEmpty
    private UUID id;

    /**
     * @return ID.
     */
    public UUID getId() {
        return id;
    }

    /**
     * @param id New ID.
     */
    public void setId(UUID id) {
        this.id = id;
    }
}
