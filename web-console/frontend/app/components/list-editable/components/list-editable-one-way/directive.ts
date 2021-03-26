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

import isMatch from 'lodash/isMatch';
import {default as ListEditableController, ID} from '../../controller';

export default function listEditableOneWay(): ng.IDirective {
    return {
        require: {
            list: 'listEditable'
        },
        bindToController: {
            onItemChange: '&?',
            onItemRemove: '&?'
        },
        controller: class Controller<T> {
            list: ListEditableController<T>;
            onItemChange: ng.ICompiledExpression;
            onItemRemove: ng.ICompiledExpression;

            $onInit() {
                this.list.save = (item: T, id: ID) => {
                    if (!isMatch(this.list.getItem(id), item)) this.onItemChange({$event: item});
                };
                this.list.remove = (id: ID) => this.onItemRemove({
                    $event: this.list.getItem(id)
                });
            }
        }
    };
}
