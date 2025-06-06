

import {Directive, AfterViewInit, Inject, ElementRef} from '@angular/core';

@Directive({selector: 'input[autofocus]'})
export class Autofocus implements AfterViewInit {
    static parameters = [[new Inject(ElementRef)]];

    constructor(private el: ElementRef<HTMLInputElement>) {}

    ngAfterViewInit() {
        setTimeout(() => this.el.nativeElement.focus(), 0);
    }
}
