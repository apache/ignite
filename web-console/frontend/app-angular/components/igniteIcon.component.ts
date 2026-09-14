

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
