

package org.apache.ignite.console.web.model;

import java.util.UUID;
import io.swagger.v3.oas.annotations.media.Schema;

import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import org.apache.ignite.internal.util.typedef.internal.S;


/**
 * Web model of toggle admin right request.
 */
public class ToggleRequest {
    /** Email. */
    @Schema(title = "User ID.", required = true)
    @NotNull
    @NotEmpty
    private UUID id;

    /** Admin flag. */
    @Schema(title = "Admin flag.", required = true)
    private boolean admin;

    /**
     * @return Email.
     */
    public UUID getId() {
        return id;
    }

    /**
     * @param id New email.
     */
    public void setId(UUID id) {
        this.id = id;
    }

    /**
     * @return Admin flag.
     */
    public boolean isAdmin() {
        return admin;
    }

    /**
     * @param admin Admin flag.
     */
    public void setAdmin(boolean admin) {
        this.admin = admin;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(ToggleRequest.class, this);
    }
}
