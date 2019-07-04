package org.apache.ignite.internal.processors.security.permission;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.ignite.internal.util.typedef.T2;

import static org.apache.ignite.internal.processors.security.permission.ActionPermission.CODE_NONE;

public class ActionDefs {

    public static ActionsBuilder builder() {
        return new ActionsBuilder();
    }

    private final List<ActionDef> defs;

    private ActionDefs(List<ActionDef> defs) {
        this.defs = defs;
    }

    public String asString(int mask) {
        StringBuilder sb = new StringBuilder();

        for (ActionDef a : defs) {
            if ((mask & a.code()) == a.code())
                sb.append(a.name()).append(',');
        }

        if (sb.length() > 0)
            sb.setLength(sb.length() - 1);

        return sb.toString();
    }

     @SuppressWarnings("StringEquality")
    private ActionDef fastSearch(String name) {
        for (ActionDef a : defs) {
            if (a.name() == name)
                return a;
        }

        return null;
    }

    public int mask(String names) {
        if (names == null || names.isEmpty())
            return CODE_NONE;

        ActionDef fast = fastSearch(names);

        if (fast != null)
            return fast.code();

        int mask = CODE_NONE;

        char[] a = names.toCharArray();

        int i = a.length - 1;

        boolean seenComma = false;

        while (i != -1) {
            char c;

            // skip whitespace
            while ((i != -1) &&
                ((c = a[i]) == ' ' || c == '\r' || c == '\n' || c == '\f' || c == '\t'))
                i--;

            // check for the known strings
            int matchlen = 0;

            boolean hasMatch = false;

            for (ActionDef def : this.defs) {
                if (def.matchName(i, a)) {
                    matchlen = def.name().length();

                    mask |= def.code();

                    hasMatch = true;

                    break;
                }
            }
            if (!hasMatch)
                throw new IllegalArgumentException("Invalid permission: " + names);

            // make sure we didn't just matchName the tail of a word
            // like "ackbarfstartlevel". Also, skip to the comma.
            seenComma = false;

            while (i >= matchlen && !seenComma) {
                switch (a[i - matchlen]) {
                    case ',':
                        seenComma = true;
                    case ' ':
                    case '\r':
                    case '\n':
                    case '\f':
                    case '\t':
                        break;
                    default:
                        throw new IllegalArgumentException("Invalid permission: " + names);
                }
                i--;
            }

            // point i at the location of the comma minus one (or -1).
            i -= matchlen;
        }

        if (seenComma)
            throw new IllegalArgumentException("Invalid permission: " + names);

        return mask;
    }

    public static class ActionsBuilder {

        List<T2<Integer, String>> defs = new ArrayList<>();

        public ActionsBuilder add(int code, String action) {
            defs.add(new T2<>(code, action));

            return this;
        }

        public ActionDefs build() {
            return new ActionDefs(
                defs.stream()
                    .map((t) -> new ActionDef(t.get1(), t.get2()))
                    .collect(Collectors.toList())
            );
        }
    }

    private static class ActionDef {

        private int code;

        private String name;

        private ActionDef(int code, String name) {
            this.code = code;
            this.name = name;
        }

        private int code() {
            return code;
        }

        private String name() {
            return name;
        }

        private boolean matchName(int pos, char[] array) {
            char[] lc = name.toCharArray();

            if (pos < lc.length - 1)
                return false;

            int cp = pos;

            for (int i = lc.length - 1; i >= 0; i--) {
                char u1 = Character.toUpperCase(array[cp]);
                char u2 = Character.toUpperCase(lc[i]);

                if (u1 != u2)
                    return false;

                cp--;
            }

            return true;
        }
    }
}
