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
