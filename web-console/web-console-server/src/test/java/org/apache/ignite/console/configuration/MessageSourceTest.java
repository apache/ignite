/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
