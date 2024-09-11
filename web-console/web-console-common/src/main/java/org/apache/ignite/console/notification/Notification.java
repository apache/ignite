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

package org.apache.ignite.console.notification;

import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Notification model.
 */
public class Notification {
    /** */
    private String origin;

    /** */
    private IRecipient rcpt;
    
    /** */
    private INotificationDescriptor desc;

    /**
     * Default constructor for serialization.
     */
    public Notification() {
        // No-op.
    }

    /**
     * Full constructor.
     *
     * @param origin Origin.
     * @param rcpt Recipient.
     * @param desc Descriptor.
     */
    public Notification(String origin, IRecipient rcpt, INotificationDescriptor desc) {
        this.origin = origin;
        this.rcpt = rcpt;
        this.desc = desc;
    }

    /**
     * @return Origin.
     */
    public String getOrigin() {
        return origin;
    }

    /**
     * @param origin Origin.
     */
    public void setOrigin(String origin) {
        this.origin = origin;
    }

    /**
     * @return Recipient.
     */
    public IRecipient getRecipient() {
        return rcpt;
    }

    /**
     * @param rcpt New recipient.
     */
    public void setRecipient(IRecipient rcpt) {
        this.rcpt = rcpt;
    }

    /**
     * @return Descriptor.
     */
    public INotificationDescriptor getDescriptor() {
        return desc;
    }

    /**
     * @param desc New descriptor.
     */
    public void setDescriptor(INotificationDescriptor desc) {
        this.desc = desc;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(Notification.class, this);
    }
}
