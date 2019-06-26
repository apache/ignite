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

import 'app/../test/angular-testbed-init';
import {ReactiveFormsModule, FormControl, Validators, FormGroup} from '@angular/forms';

import {assert} from 'chai';
import {TestBed, async, ComponentFixture, tick, fakeAsync} from '@angular/core/testing';
import {Component, Directive, NO_ERRORS_SCHEMA, Input, TemplateRef} from '@angular/core';
import { By } from '@angular/platform-browser';
import {
    FormField, FormFieldError, FormFieldHint, FormFieldTooltip,
    FormFieldRequiredMarkerStyles, FormFieldErrorStyles, FORM_FIELD_OPTIONS
} from './index';
import {FormFieldErrors} from './errors.component';
import {IgniteIcon} from '../igniteIcon.component';

suite('Angular form-field component', () => {
    @Component({selector: 'popper-content', template: ''}) class PopperContentStub {}
    @Directive({selector: '[popper]'}) class PopperStub {}
    @Component({selector: 'form-field-errors', template: ''}) class ErrorsStub {
        @Input()
        errorStyle: any;
        @Input()
        errorType: any;
        @Input()
        extraErrorMessages: any
    }
    @Component({selector: 'form-field-tooltip', template: ''}) class TooltipStub {
        @Input()
        content: any
    }

    let fixture: ComponentFixture<HostComponent>;
    @Component({
        template: `
        <div [formGroup]='form'>
            <form-field>
                <form-field-hint>Hello world!</form-field-hint>
                <label for="one">One:</label>
                <span>Foo</span>
                <input type="text" id="one" formControlName='one'>
                <i>Bar</i>
                <span formFieldOverlay>😂</span>
                <span formFieldOverlay>👌</span>
            </form-field>
            <form-field [requiredMarkerStyle]='requiredMarkerStyle' [errorStyle]='inlineError'>
                <label for="two">Two:</label>
                <input type="text" id="two" formControlName='two'>
            </form-field>
            <form-field [errorStyle]='iconError'>
                <input type="text" formControlName='three'>
            </form-field>
        </div>
        `,
        providers: [
            {
                provide: FORM_FIELD_OPTIONS,
                useValue: {
                    requiredMarkerStyle: FormFieldRequiredMarkerStyles.OPTIONAL,
                    errorStyle: FormFieldErrorStyles.INLINE
                }
            }
        ]

    })
    class HostComponent {
        form = new FormGroup({
            one: new FormControl(null, []),
            two: new FormControl(null, [Validators.required]),
            three: new FormControl(null, [Validators.required])
        });
        requiredMarkerStyle = FormFieldRequiredMarkerStyles.REQUIRED;
        inlineError = FormFieldErrorStyles.INLINE;
        iconError = FormFieldErrorStyles.ICON;
        extraMessages = {required: 'Foo'}
    }

    setup(fakeAsync(async() => {
        TestBed.configureTestingModule({
            declarations: [
                FormField, FormFieldHint,
                HostComponent,
                ErrorsStub, TooltipStub
            ],
            schemas: [NO_ERRORS_SCHEMA],
            imports: [ReactiveFormsModule]
        }).compileComponents().then(() => {
            fixture = TestBed.createComponent(HostComponent);
            fixture.detectChanges();
            tick();
            fixture.detectChanges();
        });
    }));
    test('Required marker styles', fakeAsync(() => {
        assert.ok(
            fixture.nativeElement.querySelector('form-field:nth-of-type(1)').matches('.form-field__optional'),
            'Has "optional" class when required marker mode is "optional" and required validator is not present'
        );
        assert.ok(
            fixture.nativeElement.querySelector('form-field:nth-of-type(2)').matches('.form-field__required'),
            'Has "optional" class when required marker mode is "optional" and required validator is not present'
        );
    }));
    test('Validation styles', () => {
        fixture.componentInstance.form.get('two').markAsTouched();
        fixture.componentInstance.form.get('three').markAsTouched();
        fixture.detectChanges();
        assert.ok(
            fixture.nativeElement.querySelector('form-field:nth-of-type(2)>form-field-errors'),
            'Displays errors inline when "inline" errorStyle is used.'
        );
        assert.ok(
            fixture.nativeElement.querySelector('form-field:nth-of-type(3) .input-overlay form-field-errors'),
            'Displays errors in overlay when "icon" errorStyle is used.'
        );
    });
    test('Validation message display conditions', () => {
        const hasErrors = (index: number) => !!fixture.nativeElement.querySelector(`form-field:nth-of-type(${index}) form-field-errors`);

        assert.isFalse(hasErrors(1), 'Pristine, untouched and valid shows no errors');
        fixture.componentInstance.form.get('one').markAsTouched();
        fixture.detectChanges();
        assert.isFalse(hasErrors(1), 'Pristine, touched and valid shows no errors');
        fixture.componentInstance.form.get('one').markAsUntouched();
        fixture.componentInstance.form.get('one').markAsDirty();
        fixture.detectChanges();
        assert.isFalse(hasErrors(1), 'Untouched, dirty and valid shows no errors');

        fixture.componentInstance.form.get('two').markAsTouched();
        fixture.detectChanges();
        assert.isTrue(hasErrors(2), 'Pristine, touched and invalid shows errors');
        fixture.componentInstance.form.get('two').markAsUntouched();
        fixture.componentInstance.form.get('two').markAsDirty();
        fixture.detectChanges();
        assert.isTrue(hasErrors(2), 'Dirty, untouched and invalid shows errors');
        fixture.componentInstance.form.get('two').markAsTouched();
        fixture.componentInstance.form.get('two').markAsPristine();
        fixture.detectChanges();
        assert.isTrue(hasErrors(2), 'Pristine, touched and invalid shows errors');
        fixture.componentInstance.form.get('two').markAsDirty();
        fixture.detectChanges();
        assert.isTrue(hasErrors(2), 'Dirty, touched and invalid shows errors');
    });
    test('form-field-errors intgration', () => {
        const field = (fixture.debugElement.query(By.css('form-field:nth-of-type(2)')).componentInstance as FormField);
        const extraMessages = {
            required: {} as TemplateRef<any>,
            custom: {} as TemplateRef<any>
        };
        field.addExtraErrorMessage('required', extraMessages.required);
        field.addExtraErrorMessage('custom', extraMessages.custom);
        fixture.componentInstance.form.get('two').markAsTouched();
        fixture.detectChanges();
        const errors = fixture.debugElement.query(By.css('form-field:nth-of-type(2) form-field-errors')).componentInstance as ErrorsStub;
        assert.deepEqual(
            errors.extraErrorMessages,
            extraMessages,
            'Public addExtraErrorMessage method passes extra error messages to form-field-errors'
        );
        assert.equal(
            errors.errorStyle,
            FormFieldErrorStyles.INLINE,
            'Error style value is passed to form-field-errors'
        );
        assert.equal(
            errors.errorType,
            'required',
            'Current error type is passed to form-field-errors'
        );
    });
    test('Form field hint', () => {
        const hint = fixture.debugElement.query(By.directive(FormFieldHint)).componentInstance as FormFieldHint;
        hint.popper = {};
        fixture.detectChanges();
        const tooltip = fixture.debugElement.query(By.directive(TooltipStub)).componentInstance as TooltipStub;
        assert.notOk(
            fixture.nativeElement.querySelector('form-field:nth-of-type(2) form-field-tooltip'),
            'Does not show tooltip if no hint was provided'
        );
        assert.ok(
            fixture.nativeElement.querySelector('form-field:nth-of-type(1) form-field-tooltip'),
            'Shows tooltip if hint was provided'
        );
        assert.equal(
            tooltip.content,
            hint.popper,
            'Passes hint.popper to form-field-tooltip'
        );
    });
    test('Label transclusion', () => {
        assert.ok(
            fixture.debugElement.query(By.css('form-field:nth-of-type(1) .angular-form-field__label label')),
            'It trancludes label element into label wrapper'
        );
    });
    test('Overlay', () => {
        assert.equal(
            fixture.debugElement.query(By.css('form-field:nth-of-type(1) .input-overlay')).children.length,
            2,
            'It transcludes overlay nodes into overlay container'
        );
        assert.equal(
            fixture.debugElement
                .query(By.css('form-field:nth-of-type(1) .angular-form-field__input'))
                .nativeElement.dataset.overlayItemsCount,
            '2',
            'It exposes overlay items count as data-attribute'
        );
    });
    test('Input transclusion', () => {
        assert.equal(
            fixture.debugElement
                .query(By.css('form-field:nth-of-type(1) .angular-form-field__input'))
                .nativeElement.children.length,
            4,
            'It transcludes the rest of elements into input wrapper'
        );
    });
});
