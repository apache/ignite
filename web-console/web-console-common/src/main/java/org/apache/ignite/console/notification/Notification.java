

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
