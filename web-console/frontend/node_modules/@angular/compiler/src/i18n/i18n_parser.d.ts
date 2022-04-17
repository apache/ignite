/**
 * @license
 * Copyright Google Inc. All Rights Reserved.
 *
 * Use of this source code is governed by an MIT-style license that can be
 * found in the LICENSE file at https://angular.io/license
 */
import * as html from '../ml_parser/ast';
import { InterpolationConfig } from '../ml_parser/interpolation_config';
import * as i18n from './i18n_ast';
declare type VisitNodeFn = (html: html.Node, i18n: i18n.Node) => void;
/**
 * Returns a function converting html nodes to an i18n Message given an interpolationConfig
 */
export declare function createI18nMessageFactory(interpolationConfig: InterpolationConfig): (nodes: html.Node[], meaning: string, description: string, id: string, visitNodeFn?: VisitNodeFn) => i18n.Message;
export {};
