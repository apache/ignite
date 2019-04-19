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

// Override AngularStrap "bsSelect" in order to dynamically change placeholder and class.
export default () => {
    /**
     * @param {ng.IScope} scope
     * @param {JQLite} $element
     * @param {ng.IAttributes} attrs
     * @param {[ng.INgModelController]} [ngModel]
     */
    const link = (scope, $element, attrs, [ngModel]) => {
        if (!ngModel)
            return;

        const $render = ngModel.$render;

        ngModel.$render = () => {
            if (scope.$destroyed)
                return;

            scope.$applyAsync(() => {
                $render();
                const value = ngModel.$viewValue;

                if (_.isNil(value) || (attrs.multiple && !value.length)) {
                    $element.html(attrs.placeholder);

                    $element.addClass('placeholder');
                }
                else
                    $element.removeClass('placeholder');
            });
        };
    };

    return {
        priority: 1,
        restrict: 'A',
        link,
        require: ['?ngModel']
    };
};
