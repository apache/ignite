/**
 * @license
 * Copyright Google Inc. All Rights Reserved.
 *
 * Use of this source code is governed by an MIT-style license that can be
 * found in the LICENSE file at https://angular.io/license
 */
/// <amd-module name="@angular/language-service/src/ts_plugin" />
import * as ts from 'typescript';
import * as tss from 'typescript/lib/tsserverlibrary';
export declare function getExternalFiles(project: tss.server.Project): string[] | undefined;
export declare function create(info: tss.server.PluginCreateInfo): ts.LanguageService;
