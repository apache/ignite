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
 * @type {ng.IComponentController}
 */
class OnFocusOutController {
    /** @type {OnFocusOutController} */
    parent;
    /** @type {Array<OnFocusOutController>} */
    children = [];
    /** @type {Array<string>} */
    ignoredClasses = [];
    /** @type {function} */
    igniteOnFocusOut;

    static $inject = ['$element', '$window', '$scope'];
    /**
     * @param {JQLite} $element
     * @param {ng.IWindowService} $window 
     * @param {ng.IScope} $scope
     */
    constructor($element, $window, $scope) {
        this.$element = $element;
        this.$window = $window;
        this.$scope = $scope;

        /** @param {MouseEvent|FocusEvent} e */
        this._eventHandler = (e) => {
            this.children.forEach((c) => c._eventHandler(e));
            if (this.shouldPropagate(e) && this.isFocused) {
                this.$scope.$applyAsync(() => {
                    this.igniteOnFocusOut();
                    this.isFocused = false;
                });
            }
        };
        /** @param {FocusEvent} e */
        this._onFocus = (e) => {
            this.isFocused = true;
        };
    }
    $onDestroy() {
        this.$window.removeEventListener('click', this._eventHandler, true);
        this.$window.removeEventListener('focusin', this._eventHandler, true);
        this.$element[0].removeEventListener('focus', this._onFocus, true);
        if (this.parent) this.parent.children.splice(this.parent.children.indexOf(this), 1);
        this.$element = this.$window = this._eventHandler = this._onFocus = null;
    }
    shouldPropagate(e) {
        return !this.targetHasIgnoredClasses(e) && this.targetIsOutOfElement(e);
    }
    targetIsOutOfElement(e) {
        return !this.$element.find(e.target).length;
    }
    targetHasIgnoredClasses(e) {
        return this.ignoredClasses.some((c) => e.target.classList.contains(c));
    }
    /**
     * @param {ng.IOnChangesObject} changes [description]
     */
    $onChanges(changes) {
        if (
            'ignoredClasses' in changes &&
            changes.ignoredClasses.currentValue !== changes.ignoredClasses.previousValue
        )
            this.ignoredClasses = changes.ignoredClasses.currentValue.split(' ').concat('body-overlap');
    }
    $onInit() {
        if (this.parent) this.parent.children.push(this);
    }
    $postLink() {
        this.$window.addEventListener('click', this._eventHandler, true);
        this.$window.addEventListener('focusin', this._eventHandler, true);
        this.$element[0].addEventListener('focus', this._onFocus, true);
    }
}

/**
 * @type {ng.IDirectiveFactory}
 */
export default function() {
    return {
        controller: OnFocusOutController,
        require: {
            parent: '^^?igniteOnFocusOut'
        },
        bindToController: {
            igniteOnFocusOut: '&',
            ignoredClasses: '@?igniteOnFocusOutIgnoredClasses'
        }
    };
}
