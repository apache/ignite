/**
 * @license
 * Copyright Google Inc. All Rights Reserved.
 *
 * Use of this source code is governed by an MIT-style license that can be
 * found in the LICENSE file at https://angular.io/license
 */
import { LocalResolver } from '../../compiler_util/expression_converter';
import { ConstantPool } from '../../constant_pool';
import * as core from '../../core';
import { AST, AstMemoryEfficientTransformer, BindingPipe, LiteralArray, LiteralMap } from '../../expression_parser/ast';
import * as i18n from '../../i18n/i18n_ast';
import { InterpolationConfig } from '../../ml_parser/interpolation_config';
import { LexerRange } from '../../ml_parser/lexer';
import * as o from '../../output/output_ast';
import { ParseError, ParseSourceSpan } from '../../parse_util';
import { SelectorMatcher } from '../../selector';
import { BindingParser } from '../../template_parser/binding_parser';
import * as t from '../r3_ast';
import { I18nContext } from './i18n/context';
import { invalid } from './util';
export declare function renderFlagCheckIfStmt(flags: core.RenderFlags, statements: o.Statement[]): o.IfStmt;
export declare function prepareEventListenerParameters(eventAst: t.BoundEvent, handlerName?: string | null, scope?: BindingScope | null): o.Expression[];
export declare class TemplateDefinitionBuilder implements t.Visitor<void>, LocalResolver {
    private constantPool;
    private level;
    private contextName;
    private i18nContext;
    private templateIndex;
    private templateName;
    private directiveMatcher;
    private directives;
    private pipeTypeByName;
    private pipes;
    private _namespace;
    private relativeContextFilePath;
    private i18nUseExternalIds;
    private _dataIndex;
    private _bindingContext;
    private _prefixCode;
    /**
     * List of callbacks to generate creation mode instructions. We store them here as we process
     * the template so bindings in listeners are resolved only once all nodes have been visited.
     * This ensures all local refs and context variables are available for matching.
     */
    private _creationCodeFns;
    /**
     * List of callbacks to generate update mode instructions. We store them here as we process
     * the template so bindings are resolved only once all nodes have been visited. This ensures
     * all local refs and context variables are available for matching.
     */
    private _updateCodeFns;
    /**
     * Memorizes the last node index for which a select instruction has been generated.
     * We're initializing this to -1 to ensure the `select(0)` instruction is generated before any
     * relevant update instructions.
     */
    private _lastNodeIndexWithFlush;
    /** Temporary variable declarations generated from visiting pipes, literals, etc. */
    private _tempVariables;
    /**
     * List of callbacks to build nested templates. Nested templates must not be visited until
     * after the parent template has finished visiting all of its nodes. This ensures that all
     * local ref bindings in nested templates are able to find local ref values if the refs
     * are defined after the template declaration.
     */
    private _nestedTemplateFns;
    /**
     * This scope contains local variables declared in the update mode block of the template.
     * (e.g. refs and context vars in bindings)
     */
    private _bindingScope;
    private _valueConverter;
    private _unsupported;
    private i18n;
    private _pureFunctionSlots;
    private _bindingSlots;
    private fileBasedI18nSuffix;
    private _ngContentReservedSlots;
    private _ngContentSelectorsOffset;
    private _implicitReceiverExpr;
    constructor(constantPool: ConstantPool, parentBindingScope: BindingScope, level: number, contextName: string | null, i18nContext: I18nContext | null, templateIndex: number | null, templateName: string | null, directiveMatcher: SelectorMatcher | null, directives: Set<o.Expression>, pipeTypeByName: Map<string, o.Expression>, pipes: Set<o.Expression>, _namespace: o.ExternalReference, relativeContextFilePath: string, i18nUseExternalIds: boolean);
    registerContextVariables(variable: t.Variable): void;
    buildTemplateFunction(nodes: t.Node[], variables: t.Variable[], ngContentSelectorsOffset?: number, i18n?: i18n.AST): o.FunctionExpr;
    getLocal(name: string): o.Expression | null;
    notifyImplicitReceiverUse(): void;
    i18nTranslate(message: i18n.Message, params?: {
        [name: string]: o.Expression;
    }, ref?: o.ReadVarExpr, transformFn?: (raw: o.ReadVarExpr) => o.Expression): o.ReadVarExpr;
    i18nFormatPlaceholderNames(params: {
        [name: string]: o.Expression;
    } | undefined, useCamelCase: boolean): {
        [key: string]: o.Expression;
    };
    i18nAppendBindings(expressions: AST[]): void;
    i18nBindProps(props: {
        [key: string]: t.Text | t.BoundText;
    }): {
        [key: string]: o.Expression;
    };
    i18nGenerateClosureVar(messageId: string): o.ReadVarExpr;
    i18nUpdateRef(context: I18nContext): void;
    i18nStart(span: ParseSourceSpan | null | undefined, meta: i18n.AST, selfClosing?: boolean): void;
    i18nEnd(span?: ParseSourceSpan | null, selfClosing?: boolean): void;
    visitContent(ngContent: t.Content): void;
    getNamespaceInstruction(namespaceKey: string | null): o.ExternalReference;
    addNamespaceInstruction(nsInstruction: o.ExternalReference, element: t.Element): void;
    visitElement(element: t.Element): void;
    /**
     * Adds an update instruction for an interpolated property or attribute, such as
     * `prop="{{value}}"` or `attr.title="{{value}}"`
     */
    interpolatedUpdateInstruction(instruction: o.ExternalReference, elementIndex: number, attrName: string, input: t.BoundAttribute, value: any, params: any[]): void;
    visitTemplate(template: t.Template): void;
    readonly visitReference: typeof invalid;
    readonly visitVariable: typeof invalid;
    readonly visitTextAttribute: typeof invalid;
    readonly visitBoundAttribute: typeof invalid;
    readonly visitBoundEvent: typeof invalid;
    visitBoundText(text: t.BoundText): void;
    visitText(text: t.Text): void;
    visitIcu(icu: t.Icu): null;
    private allocateDataSlot;
    getConstCount(): number;
    getVarCount(): number;
    getNgContentSelectors(): o.Expression | null;
    private bindingContext;
    private templatePropertyBindings;
    private instructionFn;
    private processStylingInstruction;
    private creationInstruction;
    private updateInstruction;
    private updateInstructionChain;
    private addSelectInstructionIfNecessary;
    private allocatePureFunctionSlots;
    private allocateBindingSlots;
    /**
     * Gets an expression that refers to the implicit receiver. The implicit
     * receiver is always the root level context.
     */
    private getImplicitReceiverExpr;
    private convertExpressionBinding;
    private convertPropertyBinding;
    /**
     * Gets a list of argument expressions to pass to an update instruction expression. Also updates
     * the temp variables state with temp variables that were identified as needing to be created
     * while visiting the arguments.
     * @param value The original expression we will be resolving an arguments list from.
     */
    private getUpdateInstructionArguments;
    private matchDirectives;
    /**
     * Prepares all attribute expression values for the `TAttributes` array.
     *
     * The purpose of this function is to properly construct an attributes array that
     * is passed into the `elementStart` (or just `element`) functions. Because there
     * are many different types of attributes, the array needs to be constructed in a
     * special way so that `elementStart` can properly evaluate them.
     *
     * The format looks like this:
     *
     * ```
     * attrs = [prop, value, prop2, value2,
     *   CLASSES, class1, class2,
     *   STYLES, style1, value1, style2, value2,
     *   BINDINGS, name1, name2, name3,
     *   TEMPLATE, name4, name5, name6,
     *   I18N, name7, name8, ...]
     * ```
     *
     * Note that this function will fully ignore all synthetic (@foo) attribute values
     * because those values are intended to always be generated as property instructions.
     */
    private prepareNonRenderAttrs;
    private toAttrsParam;
    private prepareRefsParameter;
    private prepareListenerParameter;
}
export declare class ValueConverter extends AstMemoryEfficientTransformer {
    private constantPool;
    private allocateSlot;
    private allocatePureFunctionSlots;
    private definePipe;
    private _pipeBindExprs;
    constructor(constantPool: ConstantPool, allocateSlot: () => number, allocatePureFunctionSlots: (numSlots: number) => number, definePipe: (name: string, localName: string, slot: number, value: o.Expression) => void);
    visitPipe(pipe: BindingPipe, context: any): AST;
    updatePipeSlotOffsets(bindingSlots: number): void;
    visitLiteralArray(array: LiteralArray, context: any): AST;
    visitLiteralMap(map: LiteralMap, context: any): AST;
}
/**
 * Function which is executed whenever a variable is referenced for the first time in a given
 * scope.
 *
 * It is expected that the function creates the `const localName = expression`; statement.
 */
export declare type DeclareLocalVarCallback = (scope: BindingScope, relativeLevel: number) => o.Statement[];
/**
 * This is used when one refers to variable such as: 'let abc = nextContext(2).$implicit`.
 * - key to the map is the string literal `"abc"`.
 * - value `retrievalLevel` is the level from which this value can be retrieved, which is 2 levels
 * up in example.
 * - value `lhs` is the left hand side which is an AST representing `abc`.
 * - value `declareLocalCallback` is a callback that is invoked when declaring the local.
 * - value `declare` is true if this value needs to be declared.
 * - value `localRef` is true if we are storing a local reference
 * - value `priority` dictates the sorting priority of this var declaration compared
 * to other var declarations on the same retrieval level. For example, if there is a
 * context variable and a local ref accessing the same parent view, the context var
 * declaration should always come before the local ref declaration.
 */
declare type BindingData = {
    retrievalLevel: number;
    lhs: o.Expression;
    declareLocalCallback?: DeclareLocalVarCallback;
    declare: boolean;
    priority: number;
    localRef: boolean;
};
export declare class BindingScope implements LocalResolver {
    bindingLevel: number;
    private parent;
    /** Keeps a map from local variables to their BindingData. */
    private map;
    private referenceNameIndex;
    private restoreViewVariable;
    private static _ROOT_SCOPE;
    static readonly ROOT_SCOPE: BindingScope;
    private constructor();
    get(name: string): o.Expression | null;
    /**
     * Create a local variable for later reference.
     *
     * @param retrievalLevel The level from which this value can be retrieved
     * @param name Name of the variable.
     * @param lhs AST representing the left hand side of the `let lhs = rhs;`.
     * @param priority The sorting priority of this var
     * @param declareLocalCallback The callback to invoke when declaring this local var
     * @param localRef Whether or not this is a local ref
     */
    set(retrievalLevel: number, name: string, lhs: o.Expression, priority?: number, declareLocalCallback?: DeclareLocalVarCallback, localRef?: true): BindingScope;
    getLocal(name: string): (o.Expression | null);
    notifyImplicitReceiverUse(): void;
    nestedScope(level: number): BindingScope;
    /**
     * Gets or creates a shared context variable and returns its expression. Note that
     * this does not mean that the shared variable will be declared. Variables in the
     * binding scope will be only declared if they are used.
     */
    getOrCreateSharedContextVar(retrievalLevel: number): o.ReadVarExpr;
    getSharedContextName(retrievalLevel: number): o.ReadVarExpr | null;
    maybeGenerateSharedContextVar(value: BindingData): void;
    generateSharedContextVar(retrievalLevel: number): void;
    getComponentProperty(name: string): o.Expression;
    maybeRestoreView(retrievalLevel: number, localRefLookup: boolean): void;
    restoreViewStatement(): o.Statement[];
    viewSnapshotStatements(): o.Statement[];
    isListenerScope(): boolean | null;
    variableDeclarations(): o.Statement[];
    freshReferenceName(): string;
}
/**
 * Options that can be used to modify how a template is parsed by `parseTemplate()`.
 */
export interface ParseTemplateOptions {
    /**
     * Include whitespace nodes in the parsed output.
     */
    preserveWhitespaces?: boolean;
    /**
     * How to parse interpolation markers.
     */
    interpolationConfig?: InterpolationConfig;
    /**
     * The start and end point of the text to parse within the `source` string.
     * The entire `source` string is parsed if this is not provided.
     * */
    range?: LexerRange;
    /**
     * If this text is stored in a JavaScript string, then we have to deal with escape sequences.
     *
     * **Example 1:**
     *
     * ```
     * "abc\"def\nghi"
     * ```
     *
     * - The `\"` must be converted to `"`.
     * - The `\n` must be converted to a new line character in a token,
     *   but it should not increment the current line for source mapping.
     *
     * **Example 2:**
     *
     * ```
     * "abc\
     *  def"
     * ```
     *
     * The line continuation (`\` followed by a newline) should be removed from a token
     * but the new line should increment the current line for source mapping.
     */
    escapedString?: boolean;
    /**
     * An array of characters that should be considered as leading trivia.
     * Leading trivia are characters that are not important to the developer, and so should not be
     * included in source-map segments.  A common example is whitespace.
     */
    leadingTriviaChars?: string[];
}
/**
 * Parse a template into render3 `Node`s and additional metadata, with no other dependencies.
 *
 * @param template text of the template to parse
 * @param templateUrl URL to use for source mapping of the parsed template
 * @param options options to modify how the template is parsed
 */
export declare function parseTemplate(template: string, templateUrl: string, options?: ParseTemplateOptions): {
    errors?: ParseError[];
    nodes: t.Node[];
    styleUrls: string[];
    styles: string[];
};
/**
 * Construct a `BindingParser` with a default configuration.
 */
export declare function makeBindingParser(interpolationConfig?: InterpolationConfig): BindingParser;
export declare function resolveSanitizationFn(context: core.SecurityContext, isAttribute?: boolean): o.ExternalExpr | null;
export {};
