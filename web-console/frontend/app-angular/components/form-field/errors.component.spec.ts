

import 'app/../test/angular-testbed-init';
import {FormFieldErrors} from './errors.component';
import {assert} from 'chai';
import {TestBed, ComponentFixture, fakeAsync} from '@angular/core/testing';
import {Component, NO_ERRORS_SCHEMA, ViewChild, ElementRef} from '@angular/core';
import {TranslateService} from '@ngx-translate/core';
import {of} from 'rxjs';

suite('Angular form-field-errors component', () => {
    let fixture: ComponentFixture<HostComponent>;

    @Component({
        template: `
            <form-field-errors
                errorStyle='inline'
                errorType='required'
                #inline
            ></form-field-errors>
            <form-field-errors
                errorStyle='icon'
                [errorType]='errorType'
                [extraErrorMessages]='{unique: unique}'
                #icon
            ></form-field-errors>
            <ng-template #unique>Value should be unique</ng-template>
        `
    })
    class HostComponent {
        @ViewChild('inline', {read: ElementRef, static: false})
        inline: HTMLElement;

        @ViewChild('icon', {read: ElementRef, static: false})
        icon: HTMLElement;

        errorType = 'unique';
    }

    setup(fakeAsync(async() => {
        await TestBed.configureTestingModule({
            declarations: [
                FormFieldErrors,
                HostComponent
            ],
            providers: [
                {provide: TranslateService, useValue: {get(val) {return of(val);}}}
            ],
            schemas: [NO_ERRORS_SCHEMA]
        }).compileComponents();

        fixture = TestBed.createComponent(HostComponent);
        fixture.detectChanges();
    }));

    test('Error style', () => {
        assert.ok(
            fixture.debugElement.componentInstance.inline.nativeElement.querySelector('.inline'),
            'It can show inline errors'
        );

        assert.ok(
            fixture.debugElement.componentInstance.icon.nativeElement.querySelector('.icon'),
            'It can show icon errors'
        );
    });

    test('Validation message', () => {
        assert.equal(
            'validationMessages.required',
            fixture.debugElement.componentInstance.inline.nativeElement.textContent,
            'It shows default message translation id'
        );

        assert.equal(
            'Value should be unique',
            fixture.debugElement.componentInstance.icon.nativeElement.textContent,
            'It shows custom message'
        );

        fixture.componentInstance.errorType = 'foo';
        fixture.detectChanges();

        assert.equal(
            'validationMessages.unknown',
            fixture.debugElement.componentInstance.icon.nativeElement.textContent,
            'It shows placeholder message translation id'
        );
    });
});
