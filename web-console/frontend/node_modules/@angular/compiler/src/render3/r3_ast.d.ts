/**
 * @license
 * Copyright Google Inc. All Rights Reserved.
 *
 * Use of this source code is governed by an MIT-style license that can be
 * found in the LICENSE file at https://angular.io/license
 */
import { SecurityContext } from '../core';
import { AST, BindingType, BoundElementProperty, ParsedEvent, ParsedEventType } from '../expression_parser/ast';
import { AST as I18nAST } from '../i18n/i18n_ast';
import { ParseSourceSpan } from '../parse_util';
export interface Node {
    sourceSpan: ParseSourceSpan;
    visit<Result>(visitor: Visitor<Result>): Result;
}
export declare class Text implements Node {
    value: string;
    sourceSpan: ParseSourceSpan;
    constructor(value: string, sourceSpan: ParseSourceSpan);
    visit<Result>(visitor: Visitor<Result>): Result;
}
export declare class BoundText implements Node {
    value: AST;
    sourceSpan: ParseSourceSpan;
    i18n?: import("@angular/compiler/src/i18n/i18n_ast").Message | import("@angular/compiler/src/i18n/i18n_ast").Node | undefined;
    constructor(value: AST, sourceSpan: ParseSourceSpan, i18n?: import("@angular/compiler/src/i18n/i18n_ast").Message | import("@angular/compiler/src/i18n/i18n_ast").Node | undefined);
    visit<Result>(visitor: Visitor<Result>): Result;
}
export declare class TextAttribute implements Node {
    name: string;
    value: string;
    sourceSpan: ParseSourceSpan;
    valueSpan?: ParseSourceSpan | undefined;
    i18n?: import("@angular/compiler/src/i18n/i18n_ast").Message | import("@angular/compiler/src/i18n/i18n_ast").Node | undefined;
    constructor(name: string, value: string, sourceSpan: ParseSourceSpan, valueSpan?: ParseSourceSpan | undefined, i18n?: import("@angular/compiler/src/i18n/i18n_ast").Message | import("@angular/compiler/src/i18n/i18n_ast").Node | undefined);
    visit<Result>(visitor: Visitor<Result>): Result;
}
export declare class BoundAttribute implements Node {
    name: string;
    type: BindingType;
    securityContext: SecurityContext;
    value: AST;
    unit: string | null;
    sourceSpan: ParseSourceSpan;
    valueSpan?: ParseSourceSpan | undefined;
    i18n?: import("@angular/compiler/src/i18n/i18n_ast").Message | import("@angular/compiler/src/i18n/i18n_ast").Node | undefined;
    constructor(name: string, type: BindingType, securityContext: SecurityContext, value: AST, unit: string | null, sourceSpan: ParseSourceSpan, valueSpan?: ParseSourceSpan | undefined, i18n?: import("@angular/compiler/src/i18n/i18n_ast").Message | import("@angular/compiler/src/i18n/i18n_ast").Node | undefined);
    static fromBoundElementProperty(prop: BoundElementProperty, i18n?: I18nAST): BoundAttribute;
    visit<Result>(visitor: Visitor<Result>): Result;
}
export declare class BoundEvent implements Node {
    name: string;
    type: ParsedEventType;
    handler: AST;
    target: string | null;
    phase: string | null;
    sourceSpan: ParseSourceSpan;
    handlerSpan: ParseSourceSpan;
    constructor(name: string, type: ParsedEventType, handler: AST, target: string | null, phase: string | null, sourceSpan: ParseSourceSpan, handlerSpan: ParseSourceSpan);
    static fromParsedEvent(event: ParsedEvent): BoundEvent;
    visit<Result>(visitor: Visitor<Result>): Result;
}
export declare class Element implements Node {
    name: string;
    attributes: TextAttribute[];
    inputs: BoundAttribute[];
    outputs: BoundEvent[];
    children: Node[];
    references: Reference[];
    sourceSpan: ParseSourceSpan;
    startSourceSpan: ParseSourceSpan | null;
    endSourceSpan: ParseSourceSpan | null;
    i18n?: import("@angular/compiler/src/i18n/i18n_ast").Message | import("@angular/compiler/src/i18n/i18n_ast").Node | undefined;
    constructor(name: string, attributes: TextAttribute[], inputs: BoundAttribute[], outputs: BoundEvent[], children: Node[], references: Reference[], sourceSpan: ParseSourceSpan, startSourceSpan: ParseSourceSpan | null, endSourceSpan: ParseSourceSpan | null, i18n?: import("@angular/compiler/src/i18n/i18n_ast").Message | import("@angular/compiler/src/i18n/i18n_ast").Node | undefined);
    visit<Result>(visitor: Visitor<Result>): Result;
}
export declare class Template implements Node {
    tagName: string;
    attributes: TextAttribute[];
    inputs: BoundAttribute[];
    outputs: BoundEvent[];
    templateAttrs: (BoundAttribute | TextAttribute)[];
    children: Node[];
    references: Reference[];
    variables: Variable[];
    sourceSpan: ParseSourceSpan;
    startSourceSpan: ParseSourceSpan | null;
    endSourceSpan: ParseSourceSpan | null;
    i18n?: import("@angular/compiler/src/i18n/i18n_ast").Message | import("@angular/compiler/src/i18n/i18n_ast").Node | undefined;
    constructor(tagName: string, attributes: TextAttribute[], inputs: BoundAttribute[], outputs: BoundEvent[], templateAttrs: (BoundAttribute | TextAttribute)[], children: Node[], references: Reference[], variables: Variable[], sourceSpan: ParseSourceSpan, startSourceSpan: ParseSourceSpan | null, endSourceSpan: ParseSourceSpan | null, i18n?: import("@angular/compiler/src/i18n/i18n_ast").Message | import("@angular/compiler/src/i18n/i18n_ast").Node | undefined);
    visit<Result>(visitor: Visitor<Result>): Result;
}
export declare class Content implements Node {
    selector: string;
    attributes: TextAttribute[];
    sourceSpan: ParseSourceSpan;
    i18n?: import("@angular/compiler/src/i18n/i18n_ast").Message | import("@angular/compiler/src/i18n/i18n_ast").Node | undefined;
    constructor(selector: string, attributes: TextAttribute[], sourceSpan: ParseSourceSpan, i18n?: import("@angular/compiler/src/i18n/i18n_ast").Message | import("@angular/compiler/src/i18n/i18n_ast").Node | undefined);
    visit<Result>(visitor: Visitor<Result>): Result;
}
export declare class Variable implements Node {
    name: string;
    value: string;
    sourceSpan: ParseSourceSpan;
    valueSpan?: ParseSourceSpan | undefined;
    constructor(name: string, value: string, sourceSpan: ParseSourceSpan, valueSpan?: ParseSourceSpan | undefined);
    visit<Result>(visitor: Visitor<Result>): Result;
}
export declare class Reference implements Node {
    name: string;
    value: string;
    sourceSpan: ParseSourceSpan;
    valueSpan?: ParseSourceSpan | undefined;
    constructor(name: string, value: string, sourceSpan: ParseSourceSpan, valueSpan?: ParseSourceSpan | undefined);
    visit<Result>(visitor: Visitor<Result>): Result;
}
export declare class Icu implements Node {
    vars: {
        [name: string]: BoundText;
    };
    placeholders: {
        [name: string]: Text | BoundText;
    };
    sourceSpan: ParseSourceSpan;
    i18n?: import("@angular/compiler/src/i18n/i18n_ast").Message | import("@angular/compiler/src/i18n/i18n_ast").Node | undefined;
    constructor(vars: {
        [name: string]: BoundText;
    }, placeholders: {
        [name: string]: Text | BoundText;
    }, sourceSpan: ParseSourceSpan, i18n?: import("@angular/compiler/src/i18n/i18n_ast").Message | import("@angular/compiler/src/i18n/i18n_ast").Node | undefined);
    visit<Result>(visitor: Visitor<Result>): Result;
}
export interface Visitor<Result = any> {
    visit?(node: Node): Result;
    visitElement(element: Element): Result;
    visitTemplate(template: Template): Result;
    visitContent(content: Content): Result;
    visitVariable(variable: Variable): Result;
    visitReference(reference: Reference): Result;
    visitTextAttribute(attribute: TextAttribute): Result;
    visitBoundAttribute(attribute: BoundAttribute): Result;
    visitBoundEvent(attribute: BoundEvent): Result;
    visitText(text: Text): Result;
    visitBoundText(text: BoundText): Result;
    visitIcu(icu: Icu): Result;
}
export declare class NullVisitor implements Visitor<void> {
    visitElement(element: Element): void;
    visitTemplate(template: Template): void;
    visitContent(content: Content): void;
    visitVariable(variable: Variable): void;
    visitReference(reference: Reference): void;
    visitTextAttribute(attribute: TextAttribute): void;
    visitBoundAttribute(attribute: BoundAttribute): void;
    visitBoundEvent(attribute: BoundEvent): void;
    visitText(text: Text): void;
    visitBoundText(text: BoundText): void;
    visitIcu(icu: Icu): void;
}
export declare class RecursiveVisitor implements Visitor<void> {
    visitElement(element: Element): void;
    visitTemplate(template: Template): void;
    visitContent(content: Content): void;
    visitVariable(variable: Variable): void;
    visitReference(reference: Reference): void;
    visitTextAttribute(attribute: TextAttribute): void;
    visitBoundAttribute(attribute: BoundAttribute): void;
    visitBoundEvent(attribute: BoundEvent): void;
    visitText(text: Text): void;
    visitBoundText(text: BoundText): void;
    visitIcu(icu: Icu): void;
}
export declare class TransformVisitor implements Visitor<Node> {
    visitElement(element: Element): Node;
    visitTemplate(template: Template): Node;
    visitContent(content: Content): Node;
    visitVariable(variable: Variable): Node;
    visitReference(reference: Reference): Node;
    visitTextAttribute(attribute: TextAttribute): Node;
    visitBoundAttribute(attribute: BoundAttribute): Node;
    visitBoundEvent(attribute: BoundEvent): Node;
    visitText(text: Text): Node;
    visitBoundText(text: BoundText): Node;
    visitIcu(icu: Icu): Node;
}
export declare function visitAll<Result>(visitor: Visitor<Result>, nodes: Node[]): Result[];
export declare function transformAll<Result extends Node>(visitor: Visitor<Node>, nodes: Result[]): Result[];
