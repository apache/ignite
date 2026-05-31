

package org.apache.ignite.console.event;

/**
 * Account types.
 */
public enum AccountEventType implements EventType {
    /** */
    ACCOUNT_CREATE,

    /** */
    ACCOUNT_CREATE_BY_ADMIN,

    /** */
    ACCOUNT_UPDATE,

    /** */
    ACCOUNT_DELETE,

    /** */
    PASSWORD_RESET,

    /** */
    PASSWORD_CHANGED,

    /** */
    RESET_ACTIVATION_TOKEN,
}
