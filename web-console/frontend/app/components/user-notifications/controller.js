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

export default class UserNotificationsController {
    static $inject = ['deferred', 'message', 'visible'];

    constructor(deferred, message, visible) {
        this.deferred = deferred;
        this.message = message;
        this.visible = visible;
    }

    onLoad(editor) {
        editor.setHighlightActiveLine(false);
        editor.setAutoScrollEditorIntoView(true);
        editor.$blockScrolling = Infinity;

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
        this.deferred.resolve({message: this.message, visible: this.visible});
    }
}
