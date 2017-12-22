package org.apache.ignite.internal.sql;

import org.apache.ignite.IgniteIllegalStateException;

/** FIXME */
public class SqlEscSeqParser {

    public static final int ESCAPED_CHAR_RADIX_SENTINEL = -1;

    /** FIXME */
    private enum Mode {
        /** FIXME */
        START,
        /** FIXME */
        PROCESSING,
        /** FIXME */
        FINISHED_ACCEPTED,
        /** FIXME */
        FINISHED_REJECTED,
        /** FIXME */
        ERROR
    }

    /** FIXME */
    public enum Result {
        /** FIXME */
        NEED_MORE_INPUT,
        /** FIXME */
        END_ACCEPTED,
        /** FIXME */
        END_REJECTED,
        /** FIXME */
        ERROR,
    }

    /** FIXME */
    private Mode mode;

    /** FIXME */
    private int minLen;

    /** FIXME */
    private int maxLen;

    /** FIXME */
    private int radix;

    /** FIXME */
    private final StringBuffer input = new StringBuffer();

    /** FIXME */
    public SqlEscSeqParser() {
        mode = Mode.START;
    }

    /** FIXME */
    public Result accept(char c) {

        if(mode == Mode.START)
            acceptPrefix(c);
        else
            acceptValueChar(c);

        switch (mode) {
            case START:
            case PROCESSING:
                return Result.NEED_MORE_INPUT;

            case ERROR:
                return Result.ERROR;

            case FINISHED_ACCEPTED:
                return Result.END_ACCEPTED;

            case FINISHED_REJECTED:
                return Result.END_REJECTED;

            default:
                throw new IgniteIllegalStateException("Internal error");
        }
    }

    /** FIXME */
    private void acceptPrefix(char c) {
        if (mode != Mode.START)
            throw new IgniteIllegalStateException("Internal error");

        mode = Mode.PROCESSING;

        switch (c) {
            case '0': case '1': case '2': case '3': case '4': case '5': case '6': case '7':
                minLen = 1;
                maxLen = 3;
                radix = 8;
                acceptValueChar(c);
                break;

            case 'x': case 'X':
                minLen = 1;
                maxLen = 2;
                radix = 16;
                break;

            case 'u': case 'U':
                minLen = 4;
                maxLen = 4;
                radix = 16;
                break;

            default:
                minLen = 1;
                maxLen = 1;
                radix = ESCAPED_CHAR_RADIX_SENTINEL;
                acceptValueChar(c);
        }
    }

    /** FIXME */
    private void acceptValueChar(char c) {
        int inputLen = input.length();

        if (mode == Mode.PROCESSING) {

            if (radix != ESCAPED_CHAR_RADIX_SENTINEL && !isValidDigit(c)) {

                if (inputLen < minLen)
                    mode = Mode.ERROR;
                else
                    mode = Mode.FINISHED_REJECTED;

                return;
            }
        }

        if (inputLen >= maxLen || !isValidInput(c)) {
            mode = Mode.FINISHED_REJECTED;
            return;
        }

        input.append(c);

        if (input.length() >= maxLen)
            mode = Mode.FINISHED_ACCEPTED;
    }

    /** FIXME */
    private boolean isValidDigit(char c) {
        if (radix < 10)
            return c >= '0' && c < ('0' + radix);
        else
            return (c >= '0' && c <= '9') ||
                (c >= 'a' && c < 'a' + (radix - 10)) ||
                (c >= 'A' && c < 'A' + (radix - 10));
    }

    /** FIXME */
    private boolean isValidInput(char c) {
        if (radix == 8 && input.length() == 2 && input.charAt(0) >= '4')
            return false;

        return true;
    }

    /** FIXME */
    public String convertedStr() {
        if (mode != Mode.FINISHED_ACCEPTED && mode != Mode.FINISHED_REJECTED)
            throw new IgniteIllegalStateException("Internal error");

        if (radix == ESCAPED_CHAR_RADIX_SENTINEL)
            return Character.toString(convertEscSeqChar(input.charAt(0)));
        else
            return Character.toString((char) Integer.parseInt(input.toString(), radix));
    }

    /**
     * Converts second character from escape sequence to actual character
     *
     * @param c The character after the backquote.
     * @return the character which this escape sequence represents.
     */
    private static char convertEscSeqChar(char c) {
        switch (c) {
            case 'b': return '\b';
            case 'f': return '\f';
            case 'n': return '\n';
            case 'r': return '\r';
            case 't': return '\t';
            case 'Z': return '\032';

            default:  return c;
        }
    }
}
