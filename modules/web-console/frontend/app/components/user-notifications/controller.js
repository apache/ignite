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

export default class UserNotificationsController {
    static $inject = ['deferred', 'message', 'isShown'];

    constructor(deferred, message, isShown) {
        this.deferred = deferred;
        this.message = message;
        this.isShown = isShown;
    }

    onLoad(editor) {
        editor.setHighlightActiveLine(false);
        editor.setAutoScrollEditorIntoView(true);
        editor.$blockScrolling = Infinity;

        // TODO IGNITE-5366 Ace hangs when it reaches max lines.
        // const session = editor.getSession();
        //
        // session.setUseWrapMode(true);
        // session.setOption('indentedSoftWrap', false);

        const renderer = editor.renderer;

        renderer.setPadding(7);
        renderer.setScrollMargin(7, 12);
        renderer.setHighlightGutterLine(false);
        renderer.setShowPrintMargin(false);
        renderer.setShowGutter(false);
        renderer.setOption('fontFamily', 'monospace');
        renderer.setOption('fontSize', '14px');
        renderer.setOption('minLines', '3');
        renderer.setOption('maxLines', '3');

        editor.focus();
    }

    submit() {
        this.deferred.resolve({message: this.message, isShown: this.isShown });
    }
}
