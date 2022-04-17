/**
 * @license
 * Copyright Google Inc. All Rights Reserved.
 *
 * Use of this source code is governed by an MIT-style license that can be
 * found in the LICENSE file at https://angular.io/license
 */
/// <amd-module name="@angular/language-service/src/html_info" />
export declare function elementNames(): string[];
export declare function attributeNames(element: string): string[];
export declare function attributeType(element: string, attribute: string): string | string[] | undefined;
export declare class SchemaInformation {
    schema: {
        [element: string]: {
            [property: string]: string;
        };
    };
    constructor();
    allKnownElements(): string[];
    eventsOf(elementName: string): string[];
    propertiesOf(elementName: string): string[];
    typeOf(elementName: string, property: string): string;
    private static _instance;
    static readonly instance: SchemaInformation;
}
export declare function eventNames(elementName: string): string[];
export declare function propertyNames(elementName: string): string[];
export declare function propertyType(elementName: string, propertyName: string): string;
