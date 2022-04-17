/**
 * @license
 * Copyright Google Inc. All Rights Reserved.
 *
 * Use of this source code is governed by an MIT-style license that can be
 * found in the LICENSE file at https://angular.io/license
 */
import { ParseSourceSpan } from '../parse_util';
export declare class Message {
    nodes: Node[];
    placeholders: {
        [phName: string]: string;
    };
    placeholderToMessage: {
        [phName: string]: Message;
    };
    meaning: string;
    description: string;
    id: string;
    sources: MessageSpan[];
    /**
     * @param nodes message AST
     * @param placeholders maps placeholder names to static content
     * @param placeholderToMessage maps placeholder names to messages (used for nested ICU messages)
     * @param meaning
     * @param description
     * @param id
     */
    constructor(nodes: Node[], placeholders: {
        [phName: string]: string;
    }, placeholderToMessage: {
        [phName: string]: Message;
    }, meaning: string, description: string, id: string);
}
export interface MessageSpan {
    filePath: string;
    startLine: number;
    startCol: number;
    endLine: number;
    endCol: number;
}
export interface Node {
    sourceSpan: ParseSourceSpan;
    visit(visitor: Visitor, context?: any): any;
}
export declare class Text implements Node {
    value: string;
    sourceSpan: ParseSourceSpan;
    constructor(value: string, sourceSpan: ParseSourceSpan);
    visit(visitor: Visitor, context?: any): any;
}
export declare class Container implements Node {
    children: Node[];
    sourceSpan: ParseSourceSpan;
    constructor(children: Node[], sourceSpan: ParseSourceSpan);
    visit(visitor: Visitor, context?: any): any;
}
export declare class Icu implements Node {
    expression: string;
    type: string;
    cases: {
        [k: string]: Node;
    };
    sourceSpan: ParseSourceSpan;
    expressionPlaceholder: string;
    constructor(expression: string, type: string, cases: {
        [k: string]: Node;
    }, sourceSpan: ParseSourceSpan);
    visit(visitor: Visitor, context?: any): any;
}
export declare class TagPlaceholder implements Node {
    tag: string;
    attrs: {
        [k: string]: string;
    };
    startName: string;
    closeName: string;
    children: Node[];
    isVoid: boolean;
    sourceSpan: ParseSourceSpan;
    constructor(tag: string, attrs: {
        [k: string]: string;
    }, startName: string, closeName: string, children: Node[], isVoid: boolean, sourceSpan: ParseSourceSpan);
    visit(visitor: Visitor, context?: any): any;
}
export declare class Placeholder implements Node {
    value: string;
    name: string;
    sourceSpan: ParseSourceSpan;
    constructor(value: string, name: string, sourceSpan: ParseSourceSpan);
    visit(visitor: Visitor, context?: any): any;
}
export declare class IcuPlaceholder implements Node {
    value: Icu;
    name: string;
    sourceSpan: ParseSourceSpan;
    constructor(value: Icu, name: string, sourceSpan: ParseSourceSpan);
    visit(visitor: Visitor, context?: any): any;
}
export declare type AST = Message | Node;
export interface Visitor {
    visitText(text: Text, context?: any): any;
    visitContainer(container: Container, context?: any): any;
    visitIcu(icu: Icu, context?: any): any;
    visitTagPlaceholder(ph: TagPlaceholder, context?: any): any;
    visitPlaceholder(ph: Placeholder, context?: any): any;
    visitIcuPlaceholder(ph: IcuPlaceholder, context?: any): any;
}
export declare class CloneVisitor implements Visitor {
    visitText(text: Text, context?: any): Text;
    visitContainer(container: Container, context?: any): Container;
    visitIcu(icu: Icu, context?: any): Icu;
    visitTagPlaceholder(ph: TagPlaceholder, context?: any): TagPlaceholder;
    visitPlaceholder(ph: Placeholder, context?: any): Placeholder;
    visitIcuPlaceholder(ph: IcuPlaceholder, context?: any): IcuPlaceholder;
}
export declare class RecurseVisitor implements Visitor {
    visitText(text: Text, context?: any): any;
    visitContainer(container: Container, context?: any): any;
    visitIcu(icu: Icu, context?: any): any;
    visitTagPlaceholder(ph: TagPlaceholder, context?: any): any;
    visitPlaceholder(ph: Placeholder, context?: any): any;
    visitIcuPlaceholder(ph: IcuPlaceholder, context?: any): any;
}
