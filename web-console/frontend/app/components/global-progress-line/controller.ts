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

export default class GlobalProgressLine {
    /** @type {boolean} */
    isLoading;

    static $inject = ['$element', '$document', '$scope'];

    _child: Element;

    constructor(private $element: JQLite, private $document: ng.IDocumentService, private $scope: ng.IScope) {}

    $onChanges() {
        this.$scope.$evalAsync(() => {
            if (this.isLoading) {
                this._child = this.$element[0].querySelector('.global-progress-line__progress-line');

                if (this._child)
                    this.$document[0].querySelector('web-console-header').appendChild(this._child);
            }
            else
                this.$element.hide();
        });
    }

    $onDestroy() {
        if (this._child) {
            this._child.parentElement.removeChild(this._child);
            this._child = null;
        }
    }
}
