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
