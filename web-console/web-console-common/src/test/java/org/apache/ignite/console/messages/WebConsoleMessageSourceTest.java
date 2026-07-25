

package org.apache.ignite.console.messages;

import org.junit.Assert;
import org.junit.Test;
import org.springframework.context.support.MessageSourceAccessor;

/**
 * Web console message source test.
 */
public class WebConsoleMessageSourceTest {
    /**
     * Should replace message code without args.
     */
    @Test
    public void shoudReplaceMessageCodeWithoudArgs() {
        MessageSourceAccessor messages = WebConsoleMessageSource.getAccessor();

        Assert.assertEquals("Simple message",  messages.getMessage("test.simple-message"));
    }

    /**
     * Should replace message code with args.
     */
    @Test
    public void shoudReplaceMessageCodeWithArgs() {
        WebConsoleMessageSourceAccessor messages = WebConsoleMessageSource.getAccessor();

        Assert.assertEquals("Message with args: 1, 2, 3",  messages.getMessageWithArgs("test.args-message", 1, 2, 3));
    }

    /**
     * Should replace message code with string format args.
     */
    @Test
    public void shoudReplaceMessageCodeWithStringFormatArgs() {
        WebConsoleMessageSourceAccessor messages = WebConsoleMessageSource.getAccessor();

        Assert.assertEquals("Message with args: 1.25, 2.1, 3",  messages.getMessageWithArgs("test.format-args-message", 1.25d, 2.1f, 3));
    }
}
