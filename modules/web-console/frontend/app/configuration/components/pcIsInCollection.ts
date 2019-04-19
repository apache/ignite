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

class Controller<T> {
    ngModel: ng.INgModelController;
    items: T[];

    $onInit() {
        this.ngModel.$validators.isInCollection = (item) => {
            if (!item || !this.items)
                return true;

            return this.items.includes(item);
        };
    }

    $onChanges() {
        this.ngModel.$validate();
    }
}

export default function pcIsInCollection() {
    return {
        controller: Controller,
        require: {
            ngModel: 'ngModel'
        },
        bindToController: {
            items: '<pcIsInCollection'
        }
    };
}
