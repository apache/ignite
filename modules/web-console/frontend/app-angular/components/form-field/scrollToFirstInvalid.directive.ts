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

import {Directive, HostListener, Input, Inject, ElementRef} from '@angular/core';
import {NgForm} from '@angular/forms';

@Directive({
    selector: '[scrollToFirstInvalid]'
})
export class ScrollToFirstInvalid {
    @Input()
    formGroup: NgForm;

    @HostListener('ngSubmit', ['$event'])
    onSubmit(e: Event) {
        if (this.formGroup.invalid) {
            const invalidEl = this.findFirstInvalid();
            if (invalidEl) {
                this.scrollIntoView(invalidEl);
                invalidEl.focus();
                invalidEl.blur();
                invalidEl.focus();
                setTimeout(() => this.maybeShowValidationErrorPopover(invalidEl), 0);
            }
        }
    }

    private maybeShowValidationErrorPopover(el: HTMLInputElement): void {
        try {
            el.closest('form-field').querySelector('form-field-errors ignite-icon').dispatchEvent(new MouseEvent('mouseenter'));
        }
        catch (ignored) {
            // No-op.
        }
    }

    private findFirstInvalid(): HTMLInputElement | null {
        return this.el.nativeElement.querySelector('.ng-invalid:not(panel-collapsible-angular)');
    }

    private scrollIntoView(el: Element): void {
        el.scrollIntoView({block: 'center'});
    }

    static parameters = [[new Inject(ElementRef)]];

    constructor(private el: ElementRef<HTMLFormElement>) {}
}
