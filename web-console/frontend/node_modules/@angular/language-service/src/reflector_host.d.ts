/**
 * @license
 * Copyright Google Inc. All Rights Reserved.
 *
 * Use of this source code is governed by an MIT-style license that can be
 * found in the LICENSE file at https://angular.io/license
 */
/// <amd-module name="@angular/language-service/src/reflector_host" />
import { StaticSymbolResolverHost } from '@angular/compiler';
import * as ts from 'typescript';
export declare class ReflectorHost implements StaticSymbolResolverHost {
    private readonly tsLSHost;
    private readonly hostAdapter;
    private readonly metadataReaderCache;
    private readonly moduleResolutionCache;
    private readonly fakeContainingPath;
    constructor(getProgram: () => ts.Program, tsLSHost: ts.LanguageServiceHost, _: {});
    getMetadataFor(modulePath: string): {
        [key: string]: any;
    }[] | undefined;
    moduleNameToFileName(moduleName: string, containingFile?: string): string | null;
    getOutputName(filePath: string): string;
}
