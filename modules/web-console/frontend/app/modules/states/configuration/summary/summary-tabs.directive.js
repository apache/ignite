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

export default ['summaryTabs', [() => {
    const link = (scope, $element, $attrs, [igniteUiAceTabs1, igniteUiAceTabs2]) => {
        const igniteUiAceTabs = igniteUiAceTabs1 || igniteUiAceTabs2;

        if (!igniteUiAceTabs)
            return;

        igniteUiAceTabs.onLoad = (editor) => {
            editor.setReadOnly(true);
            editor.setOption('highlightActiveLine', false);
            editor.setAutoScrollEditorIntoView(true);
            editor.$blockScrolling = Infinity;

            const renderer = editor.renderer;

            renderer.setHighlightGutterLine(false);
            renderer.setShowPrintMargin(false);
            renderer.setOption('fontFamily', 'monospace');
            renderer.setOption('fontSize', '12px');
            renderer.setOption('minLines', '25');
            renderer.setOption('maxLines', '25');

            editor.setTheme('ace/theme/chrome');
        };
    };

    return {
        priority: 1000,
        restrict: 'C',
        link,
        require: ['?igniteUiAceTabs', '?^igniteUiAceTabs']
    };
}]];
