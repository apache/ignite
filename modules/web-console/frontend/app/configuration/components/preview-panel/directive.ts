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

import ace from 'brace';
import _ from 'lodash';

previewPanelDirective.$inject = ['$interval', '$timeout'];

export default function previewPanelDirective($interval: ng.IIntervalService, $timeout: ng.ITimeoutService) {
    let animation = {editor: null, stage: 0, start: 0, stop: 0};
    let prevContent = [];

    const Range = ace.acequire('ace/range').Range;

    const _clearSelection = (editor) => {
        _.forEach(editor.session.getMarkers(false), (marker) => {
            editor.session.removeMarker(marker.id);
        });
    };

    /**
     * Switch to next stage of animation.
     */
    const _animate = () => {
        animation.stage += animation.step;

        const stage = animation.stage;
        const editor = animation.editor;

        _clearSelection(editor);

        animation.selections.forEach((selection) => {
            editor.session.addMarker(new Range(selection.start, 0, selection.stop, 0),
                'preview-highlight-' + stage, 'line', false);
        });

        if (stage === animation.finalStage) {
            editor.animatePromise = null;

            if (animation.clearOnFinal)
                _clearSelection(editor);
        }
    };

    /**
     * Selection with animation.
     *
     * @param editor Editor to show selection animation.
     * @param selections Array of selection intervals.
     * @param step Step of animation (1 or -1).
     * @param stage Start stage of animation.
     * @param finalStage Final stage of animation.
     * @param clearOnFinal Boolean flat to clear selection on animation finish.
     */
    const _fade = (editor, selections, step, stage, finalStage, clearOnFinal) => {
        const promise = editor.animatePromise;

        if (promise) {
            $interval.cancel(promise);

            _clearSelection(editor);
        }

        animation = {editor, selections, step, stage, finalStage, clearOnFinal};

        editor.animatePromise = $interval(_animate, 100, 10, false);
    };

    /**
     * Show selections with animation.
     *
     * @param editor Editor to show selection.
     * @param selections Array of selection intervals.
     */
    const _fadeIn = (editor, selections) => {
        _fade(editor, selections, 1, 0, 10, false);
    };

    /**
     * Hide selections with animation.
     *
     * @param editor Editor to show selection.
     * @param selections Array of selection intervals.
     */
    const _fadeOut = (editor, selections) => {
        _fade(editor, selections, -1, 10, 0, true);
    };

    const onChange = ([content, editor]) => {
        const {clearPromise} = editor;
        const {lines} = content;

        if (content.action === 'remove')
            prevContent = lines;
        else if (prevContent.length > 0 && lines.length > 0 && editor.attractAttention) {
            if (clearPromise) {
                $timeout.cancel(clearPromise);

                _clearSelection(editor);
            }

            const selections = [];

            let newIx = 0;
            let prevIx = 0;

            let prevLen = prevContent.length - (prevContent[prevContent.length - 1] === '' ? 1 : 0);
            let newLen = lines.length - (lines[lines.length - 1] === '' ? 1 : 0);

            const removed = newLen < prevLen;

            let skipEnd = 0;

            let selected = false;
            let scrollTo = -1;

            while (lines[newLen - 1] === prevContent[prevLen - 1] && newLen > 0 && prevLen > 0) {
                prevLen -= 1;
                newLen -= 1;

                skipEnd += 1;
            }

            while (newIx < newLen || prevIx < prevLen) {
                let start = -1;
                let stop = -1;

                // Find an index of a first line with different text.
                for (; (newIx < newLen || prevIx < prevLen) && start < 0; newIx++, prevIx++) {
                    if (newIx >= newLen || prevIx >= prevLen || lines[newIx] !== prevContent[prevIx]) {
                        start = newIx;

                        break;
                    }
                }

                if (start >= 0) {
                    // Find an index of a last line with different text by checking last string of old and new content in reverse order.
                    for (let i = start; i < newLen && stop < 0; i++) {
                        for (let j = prevIx; j < prevLen && stop < 0; j++) {
                            if (lines[i] === prevContent[j] && lines[i] !== '') {
                                stop = i;

                                newIx = i;
                                prevIx = j;

                                break;
                            }
                        }
                    }

                    if (stop < 0) {
                        stop = newLen;

                        newIx = newLen;
                        prevIx = prevLen;
                    }

                    if (start === stop) {
                        if (removed)
                            start = Math.max(0, start - 1);

                        stop = Math.min(newLen + skipEnd, stop + 1);
                    }

                    if (start <= stop) {
                        selections.push({start, stop});

                        if (!selected)
                            scrollTo = start;

                        selected = true;
                    }
                }
            }

            // Run clear selection one time.
            if (selected) {
                _fadeIn(editor, selections);

                editor.clearPromise = $timeout(() => {
                    _fadeOut(editor, selections);

                    editor.clearPromise = null;
                }, 2000);

                editor.scrollToRow(scrollTo);
            }

            prevContent = [];
        }
        else
            editor.attractAttention = true;
    };


    const link = (scope, $element, $attrs, [igniteUiAceTabs1, igniteUiAceTabs2]) => {
        const igniteUiAceTabs = igniteUiAceTabs1 || igniteUiAceTabs2;

        if (!igniteUiAceTabs)
            return;

        igniteUiAceTabs.onLoad = (editor) => {
            editor.setReadOnly(true);
            editor.setOption('highlightActiveLine', false);
            editor.setAutoScrollEditorIntoView(true);
            editor.$blockScrolling = Infinity;
            editor.attractAttention = false;

            const renderer = editor.renderer;

            renderer.setHighlightGutterLine(false);
            renderer.setShowPrintMargin(false);
            renderer.setOption('fontSize', '10px');
            renderer.setOption('maxLines', '50');

            editor.setTheme('ace/theme/chrome');
        };

        igniteUiAceTabs.onChange = onChange;
    };

    return {
        restrict: 'C',
        link,
        require: ['?igniteUiAceTabs', '?^igniteUiAceTabs']
    };
}
