

import {Injectable, Inject} from '@angular/core';
import { TranslateService } from '@ngx-translate/core';

@Injectable({
    providedIn: 'root'
})
export class VALIDATION_MESSAGES {

    static parameters = [[new Inject(TranslateService)]];

    required;
    email;
    passwordMatch;

    constructor(translate: TranslateService) {
        translate.get('validationMessages.required').subscribe((val) => this.required = val);
        translate.get('validationMessages.email').subscribe((val) => this.email = val);
        translate.get('validationMessages.passwordMatch').subscribe((val) => this.passwordMatch = val);
    }

}
