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
