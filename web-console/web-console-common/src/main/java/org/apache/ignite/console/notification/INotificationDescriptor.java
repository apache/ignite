

package org.apache.ignite.console.notification;

/**
 * Notification descriptor.
 */
public interface INotificationDescriptor {
    /**
     * @return Subject code.
     */
    public String subjectCode();

    /**
     * @return Message code.
     */
    public String messageCode();
}
