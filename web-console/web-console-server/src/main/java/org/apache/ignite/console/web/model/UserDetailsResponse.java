

package org.apache.ignite.console.web.model;

import java.util.UUID;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.NotEmpty;

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
