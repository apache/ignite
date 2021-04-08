package org.apache.ignite.internal.processors.security;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.ObjectStreamField;
import java.security.BasicPermission;
import java.security.Permission;
import java.security.PermissionCollection;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiFunction;

public abstract class IgniteComponentPermission extends BasicPermission {

    protected static final int MSK_NONE = 0x0;

    protected transient int mask;
    private String actions;

    protected IgniteComponentPermission(String name, String actions) {
        super(name, actions);
    }

    @Override public boolean implies(Permission p) {
        if (!(p instanceof IgniteComponentPermission))
            return false;

        IgniteComponentPermission that = (IgniteComponentPermission)p;

        // we get the effective mask. i.e., the "and" of this and that.
        // They must be equal to that.mask for implies to return true.
        return ((mask & that.mask) == that.mask) && super.implies(that);
    }

    @Override public String getActions() {
        if (actions == null)
            actions = actions(mask);

        return actions;
    }

    public int getMask() {
        return mask;
    }

    protected abstract String actions(int m);

    protected abstract int getMask(String actions);

    @Override public boolean equals(Object obj) {
        if (!(obj instanceof IgniteComponentPermission))
            return false;

        IgniteComponentPermission p = (IgniteComponentPermission)obj;

        return p.getName().equals(getName()) && p.mask == mask;
    }

    @Override public int hashCode() {
        return getName().hashCode() ^ mask;
    }

    private synchronized void writeObject(java.io.ObjectOutputStream s) throws IOException {
        // Write out the actions. The superclass takes care of the name
        // call getActions to make sure actions field is initialized
        if (actions == null)
            getActions();

        s.defaultWriteObject();
    }

    /**
     * readObject is called to restore the state of the PropertyPermission from a stream.
     */
    private synchronized void readObject(java.io.ObjectInputStream s) throws IOException, ClassNotFoundException {
        // Read in the action, then initialize the rest
        s.defaultReadObject();

        mask = getMask(actions);
    }

    protected abstract static class IgniteComponentPermissionCollection<T extends IgniteComponentPermission> extends PermissionCollection {

        private transient ConcurrentHashMap<String, T> perms;

        // No sync access; OK for this to be stale.
        private boolean allAllowed;

        protected IgniteComponentPermissionCollection() {
            perms = new ConcurrentHashMap<>(32);     // Capacity for default policy
            allAllowed = false;
        }

        protected abstract boolean isInstanceOf(Permission p);

        protected abstract T newInstance(String name, int mask);

        @Override public void add(Permission permission) {
            if (!isInstanceOf(permission))
                throw new IllegalArgumentException("invalid permission: " + permission);

            if (isReadOnly())
                throw new SecurityException("attempt to add a Permission to a readonly PermissionCollection");

            T p = (T)permission;
            String propName = p.getName();

            perms.merge(propName, p,
                new BiFunction<T, T, T>() {
                    @Override public T apply(T existingVal,
                        T newVal) {

                        int oldMask = existingVal.getMask();
                        int newMask = newVal.getMask();

                        if (oldMask != newMask) {
                            int effective = oldMask | newMask;

                            if (effective == newMask)
                                return newVal;

                            if (effective != oldMask)
                                return newInstance(propName, effective);
                        }

                        return existingVal;
                    }
                }
            );

            if (!allAllowed) {
                if ("*".equals(propName))
                    allAllowed = true;
            }
        }

        @Override public boolean implies(Permission permission) {
            if (!isInstanceOf(permission))
                return false;

            T p = (T)permission;
            T x;

            int desired = p.getMask();
            int effective = 0;

            // short circuit if the "*" Permission was added
            if (allAllowed) {
                x = perms.get("*");

                if (x != null) {
                    effective |= x.getMask();

                    if ((effective & desired) == desired)
                        return true;
                }
            }

            // strategy:
            // Check for full match first. Then work our way up the
            // name looking for matches on a.b.*
            String name = p.getName();

            x = perms.get(name);

            if (x != null) {
                // we have a direct hit!
                effective |= x.getMask();
                if ((effective & desired) == desired)
                    return true;
            }

            // work our way up the tree...
            int last, offset;

            offset = name.length() - 1;

            while ((last = name.lastIndexOf('.', offset)) != -1) {

                name = name.substring(0, last + 1) + "*";
                x = perms.get(name);

                if (x != null) {
                    effective |= x.getMask();
                    if ((effective & desired) == desired)
                        return true;
                }
                offset = last - 1;
            }

            // we don't have to check for "*" as it was already checked
            // at the top (allAllowed), so we just return false
            return false;
        }

        @SuppressWarnings("unchecked")
        @Override public Enumeration<Permission> elements() {
            return (Enumeration)perms.elements();
        }

        private static final ObjectStreamField[] serialPersistentFields = {
            new ObjectStreamField("permissions", Hashtable.class),
            new ObjectStreamField("all_allowed", Boolean.TYPE),
        };

        /*
         * Writes the contents of the perms field out as a Hashtable for
         * serialization compatibility with earlier releases. all_allowed
         * unchanged.
         */
        private void writeObject(ObjectOutputStream out) throws IOException {
            // Don't call out.defaultWriteObject()

            // Copy perms into a Hashtable
            Hashtable<String, T> permissions = new Hashtable<>(perms.size() * 2);
            permissions.putAll(perms);

            // Write out serializable fields
            ObjectOutputStream.PutField pfields = out.putFields();
            pfields.put("all_allowed", allAllowed);
            pfields.put("permissions", permissions);
            out.writeFields();
        }

        /*
         * Reads in a Hashtable of PropertyPermissions and saves them in the
         * perms field. Reads in all_allowed.
         */
        private void readObject(ObjectInputStream in)
            throws IOException, ClassNotFoundException {
            // Don't call defaultReadObject()

            // Read in serialized fields
            ObjectInputStream.GetField gfields = in.readFields();

            // Get all_allowed
            allAllowed = gfields.get("all_allowed", false);

            // Get permissions
//            @SuppressWarnings("unchecked")
            Hashtable<String, T> permissions = (Hashtable<String, T>)gfields.get("permissions", null);
            perms = new ConcurrentHashMap<>(permissions.size() * 2);
            perms.putAll(permissions);
        }
    }

}
