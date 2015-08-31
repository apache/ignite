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

import javafx.stage.Stage;

/**
 * Abstract base modal dialog.
 */
public abstract class ModalDialog extends Stage {
    /** Owner window. */
    protected final Stage owner;

    /**
     * @param owner Owner window.
     * @param width Window width.
     * @param height Window height.
     */
    protected ModalDialog(Stage owner, int width, int height) {
        this.owner = owner;

        this.setWidth(width);
        this.setHeight(height);
    }

    /**
     * Show modal dialog.
     */
    protected void showModal() {
        setX(owner.getX() + owner.getWidth() / 2 - getWidth() / 2);
        setY(owner.getY() + owner.getHeight() / 2 - getHeight() / 2);

        showAndWait();
    }
}