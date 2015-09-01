/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.schema.ui;

import java.util.concurrent.Callable;
import java.util.concurrent.FutureTask;
import javafx.application.Platform;
import javafx.stage.Stage;

import static org.apache.ignite.schema.ui.MessageBox.Result.NO;
import static org.apache.ignite.schema.ui.MessageBox.Result.NO_TO_ALL;
import static org.apache.ignite.schema.ui.MessageBox.Result.YES_TO_ALL;

/**
 * Callable to ask user for confirmation from non EDT thread.
 */
public class ConfirmCallable implements Callable<MessageBox.Result> {
    /** Owner window. */
    private final Stage owner;

    /** Message template. */
    private final String template;

    /** Message to show in confirmation dialog. */
    private String msg;

    /** User choice. */
    private MessageBox.Result choice = NO;

    /**
     * @param owner Owner window.
     * @param template Message template.
     */
    public ConfirmCallable(Stage owner, String template) {
        this.owner = owner;
        this.template = template;
    }

    /** {@inheritDoc} */
    @Override public MessageBox.Result call() throws Exception {
        choice = MessageBox.applyToAllChoiceDialog(owner, String.format(template, msg));

        return choice;
    }

    /**
     * Execute confirmation in EDT thread.
     *
     * @return Confirm result.
     */
    public MessageBox.Result confirm(String msg) {
        this.msg = msg;

        if (choice == YES_TO_ALL || choice == NO_TO_ALL)
            return choice;

        FutureTask<MessageBox.Result> fut = new FutureTask<>(this);

        Platform.runLater(fut);

        try {
            return fut.get();
        }
        catch (Exception ignored) {
            return NO;
        }
    }
}