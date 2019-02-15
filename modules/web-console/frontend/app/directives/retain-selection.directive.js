/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
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
