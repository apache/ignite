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

/**
 * Directive to workaround known issue with type ahead edit lost cursor position.
 * @param {ng.ITimeoutService} $timeout
 */
export default function directive($timeout) {
    let promise;

    /**
     * @param {ng.IScope} scope
     * @param {JQLite} elem  [description]
     */
    function directive(scope, elem) {
        elem.on('keydown', function(evt) {
            const key = evt.which;
            const ctrlDown = evt.ctrlKey || evt.metaKey;
            const input = this;
            let start = input.selectionStart;

            if (promise)
                $timeout.cancel(promise);

            promise = $timeout(() => {
                let setCursor = false;

                // Handle Backspace[8].
                if (key === 8 && start > 0) {
                    start -= 1;

                    setCursor = true;
                }
                // Handle Del[46].
                else if (key === 46)
                    setCursor = true;
                // Handle: Caps Lock[20], Tab[9], Shift[16], Ctrl[17], Alt[18], Esc[27], Enter[13], Arrows[37..40], Home[36], End[35], Ins[45], PgUp[33], PgDown[34], F1..F12[111..124], Num Lock[], Scroll Lock[145].
                else if (!(key === 8 || key === 9 || key === 13 || (key > 15 && key < 20) || key === 27 ||
                    (key > 32 && key < 41) || key === 45 || (key > 111 && key < 124) || key === 144 || key === 145)) {
                    // Handle: Ctrl + [A[65], C[67], V[86]].
                    if (!(ctrlDown && (key === 65 || key === 67 || key === 86))) {
                        start += 1;

                        setCursor = true;
                    }
                }

                if (setCursor)
                    input.setSelectionRange(start, start);

                promise = null;
            });
        });

        // Removes bound events in the element itself when the scope is destroyed
        scope.$on('$destroy', function() {
            elem.off('keydown');
        });
    }

    return directive;
}

directive.$inject = ['$timeout'];
