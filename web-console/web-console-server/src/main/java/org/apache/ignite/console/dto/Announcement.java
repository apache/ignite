

package org.apache.ignite.console.dto;

import java.util.UUID;

import org.apache.ignite.internal.util.typedef.internal.S;

import io.swagger.v3.oas.annotations.media.Schema;

/**
 * Announcement to show in browser.
 */
public class Announcement extends AbstractDto {
    /** */
	@Schema(title = "Announcement text.")
    private String msg;

    /** */
	@Schema(title = "Announcement visibility.")
    private boolean visible;

    /**
     * Default constructor for serialization.
     */
    public Announcement() {
        // No-op.
    }

    /**
     * Full constructor.
     *
     * @param id ID.
     * @param msg Message.
     * @param visible Visibility flag.
     */
    public Announcement(UUID id, String msg, boolean visible) {
        super(id);

        this.msg = msg;
        this.visible = visible;
    }

    /**
     * @return Notification message.
     */
    public String getMessage() {
        return msg;
    }

    /**
     * @param msg Notification message.
     */
    public void setMessage(String msg) {
        this.msg = msg;
    }

    /**
     * @return {@code true} if announcement visible.
     */
    public boolean isVisible() {
        return visible;
    }

    /**
     * @param visible Notification visibility.
     */
    public void setVisible(boolean visible) {
        this.visible = visible;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(Announcement.class, this);
    }
}
