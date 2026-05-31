

package org.apache.ignite.console.messages;

import org.springframework.context.MessageSource;
import org.springframework.context.support.MessageSourceAccessor;

import java.util.Locale;

/**
 * Wrapper on MessageSourceAccessor for adding varargs method.
 */
public class WebConsoleMessageSourceAccessor extends MessageSourceAccessor {
    /**
     * @param msgSrc Message source.
     */
    public WebConsoleMessageSourceAccessor(MessageSource msgSrc) {
        super(msgSrc);
    }

    /**
     * @param msgSrc Message source.
     * @param dfltLoc Default locale.
     */
    public WebConsoleMessageSourceAccessor(MessageSource msgSrc, Locale dfltLoc) {
        super(msgSrc, dfltLoc);
    }

    /**
     * @param code Code.
     * @param args Args.
     */
    public String getMessageWithArgs(String code, Object... args) {
        return getMessage(code, args);
    }
}
