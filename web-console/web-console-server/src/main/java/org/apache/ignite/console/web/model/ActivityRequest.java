

package org.apache.ignite.console.web.model;


import jakarta.validation.constraints.NotNull;
import org.apache.ignite.internal.util.typedef.internal.S;
import jakarta.validation.constraints.NotEmpty;

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
