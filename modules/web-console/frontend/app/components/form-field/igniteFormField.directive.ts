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

type IgniteFormFieldScope < T > = ng.IScope & ({$input: T} | {[name: string]: T});

export class IgniteFormField<T> {
    static animName = 'ignite-form-field__error-blink';
    static eventName = 'webkitAnimationEnd oAnimationEnd msAnimationEnd animationend';
    static $inject = ['$element', '$scope'];
    onAnimEnd: () => any | null

    constructor(private $element: JQLite, private $scope: IgniteFormFieldScope<T>) {}

    $postLink() {
        this.onAnimEnd = () => this.$element.removeClass(IgniteFormField.animName);
        this.$element.on(IgniteFormField.eventName, this.onAnimEnd);
    }

    $onDestroy() {
        this.$element.off(IgniteFormField.eventName, this.onAnimEnd);
        this.$element = this.onAnimEnd = null;
    }

    notifyAboutError() {
        if (!this.$element)
            return;

        this.$element.addClass(IgniteFormField.animName);
        this.$element.find('.form-field__error [bs-tooltip]').trigger('mouseenter');
    }

    hideError() {
        if (!this.$element)
            return;

        this.$element.find('.form-field__error [bs-tooltip]').trigger('mouseleave');
    }

    /**
     * Exposes control in $scope
     * @param {ng.INgModelController} control
     */
    exposeControl(control, name = '$input') {
        this.$scope[name] = control;
        this.$scope.$on('$destroy', () => this.$scope[name] = null);
    }
}

export function directive<T>(): ng.IDirective<IgniteFormFieldScope<T>> {
    return {
        restrict: 'C',
        controller: IgniteFormField,
        scope: true
    };
}
