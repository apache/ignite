package org.apache.ignite.testframework;

import org.jetbrains.annotations.NotNull;

import java.util.regex.Pattern;

/** FIXME */
public class StrOrRegex {
    private final String s;
    private final Pattern p;

    /** FIXME */
    private StrOrRegex(@NotNull String s) {
        assert s != null;
        this.s = s;
        this.p = null;
    }

    /** FIXME */
    private StrOrRegex(@NotNull Pattern p) {
        assert p != null;
        this.s = null;
        this.p = p;
    }

    /** FIXME */
    public String s() {
        assert s != null;
        return s;
    }

    /** FIXME */
    public Pattern p() {
        assert p != null;
        return p;
    }

    /** FIXME */
    public boolean isString() {
        return s != null;
    }

    /** FIXME */
    public boolean isRegex() {
        return p != null;
    }

    /** FIXME */
    public boolean isContainedIn(String input) {
        if (isString())
            return input.contains(s());
        else
            return p().matcher(input).find();
    }

    @Override public String toString() {
        return isString() ? ('"' + s() + '"') : ('/' + p().toString() + '/');
    }

    /** FIXME */
    public static StrOrRegex of(String s) {
        return new StrOrRegex(s);
    }

    /** FIXME */
    public static StrOrRegex of(Pattern p) {
        return new StrOrRegex(p);
    }

    /** FIXME */
    public static StrOrRegex ofNullable(String s) {
        return s == null ? null : of(s);
    }

    /** FIXME */
    public static StrOrRegex ofNullable(Pattern p) {
        return p == null ? null : of(p);
    }

    /** FIXME */
    public static StrOrRegex ofRegex(String re) {
        return of(Pattern.compile(re));
    }
}
