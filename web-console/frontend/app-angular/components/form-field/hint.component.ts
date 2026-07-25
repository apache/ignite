

import {Component, ViewChild} from '@angular/core';
import {PopperContent} from 'ngx-popper';

@Component({
    selector: 'form-field-hint',
    template: `
        <popper-content>
            <ng-content></ng-content>
        </popper-content>
    `
})
export class FormFieldHint {
    @ViewChild(PopperContent, {static: true})
    popper: PopperContent
}
