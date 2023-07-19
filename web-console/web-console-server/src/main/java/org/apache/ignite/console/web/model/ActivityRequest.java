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


import javax.validation.constraints.NotNull;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.hibernate.validator.constraints.NotEmpty;

import io.swagger.v3.oas.annotations.media.Schema;

/**
 * Web model of activity update request.
 */
public class ActivityRequest {
    /** Group. */
    @Schema(title = "Activity group.", required = true)
    @NotNull
    @NotEmpty
    private String grp;

    /** Activity. */
    @Schema(title = "Activity name.", required = true)
    @NotNull
    @NotEmpty
    private String act;

    /**
     * @return Group.
     */
    public String getGroup() {
        return grp;
    }

    /**
     * @param grp Group.
     */
    public void setGroup(String grp) {
        this.grp = grp;
    }

    /**
     * @return Action.
     */
    public String getAction() {
        return act;
    }

    /**
     * @param act Action.
     */
    public void setAction(String act) {
        this.act = act;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(ActivityRequest.class, this);
    }
}
