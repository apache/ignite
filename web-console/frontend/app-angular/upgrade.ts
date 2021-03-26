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

import {UpgradeComponent} from '@angular/upgrade/static';
import {Directive, ElementRef, Injector, Inject, Input} from '@angular/core';

@Directive({
    selector: 'global-progress-line'
})
export class GlobalProgressLine extends UpgradeComponent {
    static parameters = [[new Inject(ElementRef)], [new Inject(Injector)]];

    constructor(elRef: ElementRef, injector: Injector) {
        super('globalProgressLine', elRef, injector);
    }

    @Input()
    isLoading: boolean
}

export const upgradedComponents = [
    GlobalProgressLine
];
