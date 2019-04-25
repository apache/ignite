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

import {StatusOptions, StatusOption} from './index';

interface Changes extends ng.IOnChangesObject {
    value: ng.IChangesObject<string>,
    options: ng.IChangesObject<StatusOptions>
}

const UNIVERSAL_CLASSNAME = 'status-output';

export class Status implements ng.IComponentController, ng.IOnChanges, ng.IPostLink, ng.IOnDestroy {
    static $inject = ['$element'];

    value: string;
    options: StatusOptions;
    status: StatusOption | undefined;
    statusClassName: string | undefined;

    constructor(private el: JQLite) {}

    $postLink() {
        this.el[0].classList.add(UNIVERSAL_CLASSNAME);
    }

    $onDestroy() {
        delete this.el;
    }

    $onChanges(changes: Changes) {
        if ('value' in changes) {
            this.status = this.options.find((option) => option.value === this.value);

            if (this.status)
                this.statusClassName = `${UNIVERSAL_CLASSNAME}__${this.status.level.toLowerCase()}`;
        }
    }
}
