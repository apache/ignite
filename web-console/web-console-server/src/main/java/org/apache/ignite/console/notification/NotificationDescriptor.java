

package org.apache.ignite.console.notification;

/**
 * Notification descriptors.
 */
public enum NotificationDescriptor implements INotificationDescriptor {
    /** */
    ADMIN_WELCOME_LETTER(
        "notifications.admin.welcome.letter.sbj",
        "notifications.admin.welcome.letter.msg"
    ),

    /** */
    WELCOME_LETTER(
        "notifications.welcome.letter.sbj",
        "notifications.welcome.letter.msg"
    ),

    /** */
    ACTIVATION_LINK(
        "notifications.activation.link.sbj",
        "notifications.activation.link.msg"
    ),

    /** */
    PASSWORD_RESET(
        "notifications.password.reset.sbj",
        "notifications.password.reset.msg"
    ),

    /** */
    PASSWORD_CHANGED(
        "notifications.password.changed.sbj",
        "notifications.password.changed.msg"
    ),

    /** */
    ACCOUNT_DELETED(
        "notifications.account.deleted.sbj",
        "notifications.account.deleted.msg"
    );

    /** */
    private final String sbjCode;

    /** */
    private final String msgCode;

    /**
     * @param sbjCode Subject code.
     * @param msgCode Message code.
     */
    NotificationDescriptor(String sbjCode, String msgCode) {
        this.sbjCode = sbjCode;
        this.msgCode = msgCode;
    }

    /** {@inheritDoc} */
    @Override public String subjectCode() {
        return sbjCode;
    }

    /** {@inheritDoc} */
    @Override public String messageCode() {
        return msgCode;
    }
}
