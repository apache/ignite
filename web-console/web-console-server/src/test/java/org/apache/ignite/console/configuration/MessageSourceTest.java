

package org.apache.ignite.console.configuration;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.support.MessageSourceAccessor;
import org.springframework.test.context.junit4.SpringRunner;

import static org.apache.ignite.console.notification.NotificationDescriptor.ADMIN_WELCOME_LETTER;
import static org.junit.Assert.assertNotEquals;

/** */
@RunWith(SpringRunner.class)
@SpringBootTest
public class MessageSourceTest {
    /** Message source accessor. */
    @Autowired
    private MessageSourceAccessor accessor;

    /** */
    @Test
    public void getMessage() {
        String code = ADMIN_WELCOME_LETTER.messageCode();

        String msg = accessor.getMessage(code, null, code);

        assertNotEquals(code, msg);
    }
}
