

package org.apache.ignite.console.notification;

/**
 * Recipient for notification.
 */
public interface IRecipient {
    /**
     * @return Email.
     */
    public String getEmail();

    /**
     * @return Phone.
     */
    public String getPhone();
}
