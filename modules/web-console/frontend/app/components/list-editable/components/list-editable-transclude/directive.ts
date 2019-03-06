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

import {default as ListEditable, ItemScope} from '../../controller';

type TranscludedScope<T> = {$form: ng.IFormController, $item?: T} & ng.IScope

/**
 * Transcludes list-editable slots and proxies item and form scope values to the slot scope,
 * also provides a way to retrieve internal list-editable ng-repeat $index by controller getter.
 * User can provide an alias for $item by setting item-name attribute on transclusion slot element.
 */
export class ListEditableTransclude<T> {
    /**
     * Transcluded slot name.
     */
    slot: string;

    list: ListEditable<T>;

    static $inject = ['$scope', '$element'];

    constructor(private $scope: ItemScope<T>, private $element: JQLite) {}

    $postLink() {
        this.list.$transclude((clone, transcludedScope: TranscludedScope<T>) => {
            // Ilya Borisov: at first I tried to use a slave directive to get value from
            // attribute and set it to ListEditableTransclude controller, but it turns out
            // this directive would run after list-editable-transclude, so that approach
            // doesn't work. That's why I decided to access a raw DOM attribute instead.
            const itemName = clone.attr('item-name') || '$item';

            // We don't want to keep references to any parent scope objects
            transcludedScope.$on('$destroy', () => {
                delete transcludedScope[itemName];
                delete transcludedScope.$form;
            });

            Object.defineProperties(transcludedScope, {
                [itemName]: {
                    get: () => {
                        // Scope might get destroyed
                        if (!this.$scope)
                            return;

                        return this.$scope.item;
                    },
                    set: (value) => {
                        // There are two items: the original one from collection and an item from
                        // cache that will be saved, so the latter should be the one we set.
                        if (!this.$scope)
                            return;

                        this.$scope.item = value;
                    },
                    // Allows to delete property later
                    configurable: true
                },
                $form: {
                    get: () => {
                        // Scope might get destroyed
                        if (!this.$scope)
                            return;

                        return this.$scope.form;
                    },
                    // Allows to delete property later
                    configurable: true
                }
            });

            this.$element.append(clone);
        }, null, this.slot);
    }

    /**
     * Returns list-editable ng-repeat $index.
     */
    get $index() {
        if (!this.$scope)
            return;

        return this.$scope.$index;
    }

    $onDestroy() {
        this.$scope = this.$element = null;
    }
}

export function listEditableTransclude() {
    return {
        restrict: 'A',
        require: {
            list: '^listEditable'
        },
        scope: false,
        controller: ListEditableTransclude,
        bindToController: {
            slot: '@listEditableTransclude'
        }
    };
}
