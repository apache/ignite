

import {Component, Input} from '@angular/core';
import {PopperContent} from 'ngx-popper';

@Component({
    selector: 'form-field-tooltip',
    templateUrl: './tooltip.template.html',
    styleUrls: ['./tooltip.style.url.scss']
})
export class FormFieldTooltip {
    @Input()
    content: PopperContent
}
