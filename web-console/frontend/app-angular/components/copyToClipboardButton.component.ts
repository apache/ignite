

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
