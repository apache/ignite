package org.apache.ignite.internal.processors.security.permission;

import java.io.IOException;
import java.security.BasicPermission;
import java.security.Permission;

public abstract class ActionPermission extends BasicPermission {
    /** No actions code. */
    protected static final int CODE_NONE = 0x0;

    /** The actions mask. */
    private transient int mask;

    /** The canonical string representation of the actions. */
    private String actions;

    protected ActionPermission(String name, String actions) {
        super(name, actions);

        init(actions);
    }

    private void init(String actions) {
        if (getName() == null)
            throw new NullPointerException("Name can't be null.");

        int m = actionDefs().mask(actions);

        if ((m & codeAll()) != m && m == CODE_NONE)
            throw new IllegalArgumentException("Invalid actions mask.");

        mask = m;
    }

    @Override public final boolean implies(Permission permission) {
        if (!(permission instanceof ActionPermission))
            return false;

        ActionPermission that = (ActionPermission)permission;

        // we get the effective mask. i.e., the "and" of this and that.
        // They must be equal to that.mask for implies to return true.
        return ((mask & that.mask) == that.mask) && super.implies(that);
    }

    /**
     * Returns the "canonical string representation" of the actions. That is, this method always returns present actions
     * in the following order: execute, cancel. For example, if this TaskPermission object allows both execute and
     * cancel actions, a call to <code>getActions</code> will return the string "execute,cancel".
     *
     * @return the canonical string representation of the actions.
     */
    @Override public String getActions() {
        if (actions == null)
            actions = actionDefs().asString(mask);

        return actions;
    }

    protected abstract ActionDefs actionDefs();

    protected abstract int codeAll();

    protected String hasMask(int mask, int action, String res) {
        if ((mask & action) == action)
            return res + ',';
        return "";
    }

    /**
     * Return the current action mask.
     *
     * @return the actions mask.
     */
    int mask() {
        return mask;
    }

    @Override public int hashCode() {
        int h = 31 * 17 + getName().hashCode();

        return 31 * h + mask;
    }

    @Override public boolean equals(Object obj) {
        if (obj == this)
            return true;

        if (obj == null || obj.getClass() != getClass())
            return false;

        ActionPermission ap = (ActionPermission)obj;

        return getName().equals(ap.getName()) && mask == ap.mask;
    }

    /**
     * WriteObject is called to save the state of the TaskPermission to a stream. The actions are serialized, and the
     * superclass takes care of the name.
     */
    private synchronized void writeObject(java.io.ObjectOutputStream s) throws IOException {
        // Write out the actions. The superclass takes care of the name
        // call getActions to make sure actions field is initialized
        if (actions == null)
            getActions();

        s.defaultWriteObject();
    }

    /**
     * ReadObject is called to restore the state of the TaskPermission from a stream.
     */
    private synchronized void readObject(java.io.ObjectInputStream s) throws IOException, ClassNotFoundException {
        // Read in the action, then initialize the rest
        s.defaultReadObject();

        init(actions);
    }
}
