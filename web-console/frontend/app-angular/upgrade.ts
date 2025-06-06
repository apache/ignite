

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
