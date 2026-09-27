

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
