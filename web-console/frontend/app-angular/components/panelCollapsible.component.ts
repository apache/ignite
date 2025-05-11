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

import {Component, Input, forwardRef, HostBinding} from '@angular/core';
import {NG_VALUE_ACCESSOR, ControlValueAccessor} from '@angular/forms';

@Component({
    selector: 'panel-collapsible-angular',
    template: `
        <div class="heading" (click)='toggle()' (keyup.enter)='toggle()' (keyup.space)='toggle()' tabindex='0'>
            <ignite-icon class='status' [name]='opened ? "collapse" : "expand"'></ignite-icon>
            <div class="title">
                <ng-content select='[panelTitle]'></ng-content>
            </div>
            <div class="description">
                <ng-content select='[panelDescription]'></ng-content>
            </div>
            <div class="actions" (click)='$event.stopPropagation()'>
                <ng-content select='[panelActions]'></ng-content>
            </div>
        </div>
        <div class="content" [hidden]='!opened'>
            <ng-content></ng-content>
        </div>
    `,
    styles: [`
        :host {
            display: flex;
            flex-direction: column;
            border-radius: 0 0 4px 4px;
            background-color: #ffffff;
            box-shadow: 0 2px 4px 0 rgba(0, 0, 0, 0.2);
        }

        :host[disabled] {
            opacity: 0.5;
        }

        :host > .heading .status {
            margin-right: 10px;
            color: #757575;
            width: 13px;
            height: 13px;
        }

        :host > .heading {
            padding: 30px 20px 30px;
            color: #393939;
            display: flex;
            flex-direction: row;
            align-items: baseline;
            user-select: none;
            cursor: pointer;
            line-height: 1.42857;
        }

        :host > .heading .title {
            font-size: 16px;
            margin-right: 8px;
            white-space: nowrap;
            overflow: hidden;
            text-overflow: ellipsis;
        }

        :host > .heading .description {
            font-size: 12px;
            color: #757575;
            flex: 1 1;
        }

        :host > .heading .actions {
            margin-left: auto;
            flex: 0 0 auto;

            & > panel-actions {
                display: flex;
                flex-direction: row;

                & > * {
                    flex: 0 0 auto;
                    margin-left: 10px;
                }
            }
        }

        :host > .content {
            border-top: 1px solid #dddddd;
            padding: 15px 20px;
        }
    `],
    providers: [{
        provide: NG_VALUE_ACCESSOR,
        useExisting: forwardRef(() => PanelCollapsible),
        multi: true
    }]
})
export class PanelCollapsible implements ControlValueAccessor {
    @Input()
    opened: boolean = false;

    @HostBinding('attr.open')
    private get openAttr() {
        return this.opened ? '' : void 0;
    }

    @Input()
    disabled: string;

    private _onChange?: (value: PanelCollapsible['opened']) => void;

    toggle() {
        if (this.opened)
            this.close();
        else
            this.open();
    }

    open() {
        if (this.disabled) return;
        this.opened = true;
        if (this._onChange) this._onChange(this.opened);
    }

    close() {
        if (this.disabled) return;
        this.opened = false;
        if (this._onChange) this._onChange(this.opened);
    }

    writeValue(value: PanelCollapsible['opened']) {
        this.opened = value;
    }

    registerOnChange(fn) {
        this._onChange = fn;
    }

    registerOnTouched() {}
}
