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

import {Component, Input, Inject} from '@angular/core';
import {default as IgniteCopyToClipboardFactory} from 'app/services/CopyToClipboard.service';

@Component({
    selector: 'copy-to-clipboard-button',
    template: `
        <button
            (click)='copy()'
            [popper]='content'
            popperApplyClass='ignite-popper,ignite-popper__tooltip'
            popperTrigger='hover'
            popperAppendTo='body'
            type='button'
        >
            <ignite-icon name='copy'></ignite-icon>
        </button>
        <popper-content #content><ng-content></ng-content></popper-content>
    `,
    styles: [`
        :host {
            display: inline-flex;
            align-items: center;
            justify-content: center;
        }
        button {
            border: none;
            margin: 0;
            padding: 0;
            background: none;
            display: inline-flex;
        }
        button:hover, button:active {
            color: #0067b9;
        }
    `]
})
export class CopyToClipboardButton {
    @Input()
    value: string;

    private copy() {
        this.IgniteCopyToClipboard.copy(this.value);
    }

    static parameters = [[new Inject('IgniteCopyToClipboard')]];

    constructor(private IgniteCopyToClipboard: ReturnType<typeof IgniteCopyToClipboardFactory>) {}
}
