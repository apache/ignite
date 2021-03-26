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
