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

import {Component, Inject, Input, OnChanges} from '@angular/core';
import {default as IgniteIconsService} from 'app/components/ignite-icon/service';

@Component({
    selector: 'ignite-icon',
    template: `<svg [attr.viewBox]='viewbox'><svg:use [attr.xlink:href]="url" [attr.href]='url'></svg:use></svg>`,
    styles: [`
        :host {
            width: 16px;
            height: 16px;
            display: inline-flex;
        }
        svg {
            width: 100%;
            height: 100%;
        }
    `]
})
export class IgniteIcon implements OnChanges {
    @Input()
    name: string;
    viewbox: string;

    static parameters = [
        [new Inject('IgniteIcon')]
    ];

    constructor(private icons: IgniteIconsService) {}

    get url() {
        return `${window.location.href}#${this.name}`;
    }

    ngOnChanges() {
        const icon = this.icons.getIcon(this.name);
        if (icon)
            this.viewbox = icon.viewBox;
    }
}
