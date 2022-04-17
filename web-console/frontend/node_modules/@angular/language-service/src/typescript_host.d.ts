/**
 * @license
 * Copyright Google Inc. All Rights Reserved.
 *
 * Use of this source code is governed by an MIT-style license that can be
 * found in the LICENSE file at https://angular.io/license
 */
/// <amd-module name="@angular/language-service/src/typescript_host" />
import { CompileMetadataResolver, HtmlParser, NgAnalyzedModules, ParseTreeResult, ResourceLoader } from '@angular/compiler';
import * as ts from 'typescript';
import { AstResult, TemplateInfo } from './common';
import { Declarations, LanguageService, LanguageServiceHost, TemplateSource, TemplateSources } from './types';
/**
 * Create a `LanguageServiceHost`
 */
export declare function createLanguageServiceFromTypescript(host: ts.LanguageServiceHost, service: ts.LanguageService): LanguageService;
/**
 * The language service never needs the normalized versions of the metadata. To avoid parsing
 * the content and resolving references, return an empty file. This also allows normalizing
 * template that are syntatically incorrect which is required to provide completions in
 * syntactically incorrect templates.
 */
export declare class DummyHtmlParser extends HtmlParser {
    parse(): ParseTreeResult;
}
/**
 * Avoid loading resources in the language servcie by using a dummy loader.
 */
export declare class DummyResourceLoader extends ResourceLoader {
    get(url: string): Promise<string>;
}
/**
 * An implementation of a `LanguageServiceHost` for a TypeScript project.
 *
 * The `TypeScriptServiceHost` implements the Angular `LanguageServiceHost` using
 * the TypeScript language services.
 *
 * @publicApi
 */
export declare class TypeScriptServiceHost implements LanguageServiceHost {
    private host;
    private tsService;
    private _resolver;
    private _staticSymbolCache;
    private _summaryResolver;
    private _staticSymbolResolver;
    private _reflector;
    private _reflectorHost;
    private _checker;
    private context;
    private lastProgram;
    private modulesOutOfDate;
    private analyzedModules;
    private fileToComponent;
    private templateReferences;
    private collectedErrors;
    private fileVersions;
    constructor(host: ts.LanguageServiceHost, tsService: ts.LanguageService);
    /**
     * Angular LanguageServiceHost implementation
     */
    readonly resolver: CompileMetadataResolver;
    getTemplateReferences(): string[];
    getTemplateAt(fileName: string, position: number): TemplateSource | undefined;
    getAnalyzedModules(): NgAnalyzedModules;
    private ensureAnalyzedModules;
    getTemplates(fileName: string): TemplateSources;
    getDeclarations(fileName: string): Declarations;
    getSourceFile(fileName: string): ts.SourceFile | undefined;
    updateAnalyzedModules(): void;
    private readonly program;
    private readonly checker;
    private validate;
    private clearCaches;
    private ensureTemplateMap;
    private getSourceFromDeclaration;
    private getSourceFromNode;
    private getSourceFromType;
    private readonly reflectorHost;
    private collectError;
    private readonly staticSymbolResolver;
    private readonly reflector;
    private getTemplateClassFromStaticSymbol;
    private static missingTemplate;
    /**
     * Given a template string node, see if it is an Angular template string, and if so return the
     * containing class.
     */
    private getTemplateClassDeclFromNode;
    private getCollectedErrors;
    private getDeclarationFromNode;
    private stringOf;
    private findNode;
    getTemplateAstAtPosition(fileName: string, position: number): TemplateInfo | undefined;
    getTemplateAst(template: TemplateSource, contextFile: string): AstResult;
}
