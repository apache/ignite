/**
 * @license
 * Copyright Google Inc. All Rights Reserved.
 *
 * Use of this source code is governed by an MIT-style license that can be
 * found in the LICENSE file at https://angular.io/license
 */
(function (factory) {
    if (typeof module === "object" && typeof module.exports === "object") {
        var v = factory(require, exports);
        if (v !== undefined) module.exports = v;
    }
    else if (typeof define === "function" && define.amd) {
        define("@angular/core/schematics/utils/ng_component_template", ["require", "exports", "fs", "path", "typescript", "@angular/core/schematics/utils/line_mappings", "@angular/core/schematics/utils/ng_decorators", "@angular/core/schematics/utils/typescript/functions", "@angular/core/schematics/utils/typescript/property_name"], factory);
    }
})(function (require, exports) {
    "use strict";
    Object.defineProperty(exports, "__esModule", { value: true });
    const fs_1 = require("fs");
    const path_1 = require("path");
    const ts = require("typescript");
    const line_mappings_1 = require("@angular/core/schematics/utils/line_mappings");
    const ng_decorators_1 = require("@angular/core/schematics/utils/ng_decorators");
    const functions_1 = require("@angular/core/schematics/utils/typescript/functions");
    const property_name_1 = require("@angular/core/schematics/utils/typescript/property_name");
    /**
     * Visitor that can be used to determine Angular templates referenced within given
     * TypeScript source files (inline templates or external referenced templates)
     */
    class NgComponentTemplateVisitor {
        constructor(typeChecker) {
            this.typeChecker = typeChecker;
            this.resolvedTemplates = [];
        }
        visitNode(node) {
            if (node.kind === ts.SyntaxKind.ClassDeclaration) {
                this.visitClassDeclaration(node);
            }
            ts.forEachChild(node, n => this.visitNode(n));
        }
        visitClassDeclaration(node) {
            if (!node.decorators || !node.decorators.length) {
                return;
            }
            const ngDecorators = ng_decorators_1.getAngularDecorators(this.typeChecker, node.decorators);
            const componentDecorator = ngDecorators.find(dec => dec.name === 'Component');
            // In case no "@Component" decorator could be found on the current class, skip.
            if (!componentDecorator) {
                return;
            }
            const decoratorCall = componentDecorator.node.expression;
            // In case the component decorator call is not valid, skip this class declaration.
            if (decoratorCall.arguments.length !== 1) {
                return;
            }
            const componentMetadata = functions_1.unwrapExpression(decoratorCall.arguments[0]);
            // Ensure that the component metadata is an object literal expression.
            if (!ts.isObjectLiteralExpression(componentMetadata)) {
                return;
            }
            const sourceFile = node.getSourceFile();
            const sourceFileName = sourceFile.fileName;
            // Walk through all component metadata properties and determine the referenced
            // HTML templates (either external or inline)
            componentMetadata.properties.forEach(property => {
                if (!ts.isPropertyAssignment(property)) {
                    return;
                }
                const propertyName = property_name_1.getPropertyNameText(property.name);
                // In case there is an inline template specified, ensure that the value is statically
                // analyzable by checking if the initializer is a string literal-like node.
                if (propertyName === 'template' && ts.isStringLiteralLike(property.initializer)) {
                    // Need to add an offset of one to the start because the template quotes are
                    // not part of the template content.
                    const templateStartIdx = property.initializer.getStart() + 1;
                    const filePath = path_1.resolve(sourceFileName);
                    this.resolvedTemplates.push({
                        filePath: filePath,
                        container: node,
                        content: property.initializer.text,
                        inline: true,
                        start: templateStartIdx,
                        getCharacterAndLineOfPosition: pos => ts.getLineAndCharacterOfPosition(sourceFile, pos + templateStartIdx)
                    });
                }
                if (propertyName === 'templateUrl' && ts.isStringLiteralLike(property.initializer)) {
                    const templatePath = path_1.resolve(path_1.dirname(sourceFileName), property.initializer.text);
                    // In case the template does not exist in the file system, skip this
                    // external template.
                    if (!fs_1.existsSync(templatePath)) {
                        return;
                    }
                    const fileContent = fs_1.readFileSync(templatePath, 'utf8');
                    const lineStartsMap = line_mappings_1.computeLineStartsMap(fileContent);
                    this.resolvedTemplates.push({
                        filePath: templatePath,
                        container: node,
                        content: fileContent,
                        inline: false,
                        start: 0,
                        getCharacterAndLineOfPosition: pos => line_mappings_1.getLineAndCharacterFromPosition(lineStartsMap, pos),
                    });
                }
            });
        }
    }
    exports.NgComponentTemplateVisitor = NgComponentTemplateVisitor;
});
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoibmdfY29tcG9uZW50X3RlbXBsYXRlLmpzIiwic291cmNlUm9vdCI6IiIsInNvdXJjZXMiOlsiLi4vLi4vLi4vLi4vLi4vLi4vLi4vcGFja2FnZXMvY29yZS9zY2hlbWF0aWNzL3V0aWxzL25nX2NvbXBvbmVudF90ZW1wbGF0ZS50cyJdLCJuYW1lcyI6W10sIm1hcHBpbmdzIjoiQUFBQTs7Ozs7O0dBTUc7Ozs7Ozs7Ozs7OztJQUVILDJCQUE0QztJQUM1QywrQkFBc0M7SUFDdEMsaUNBQWlDO0lBRWpDLGdGQUFzRjtJQUN0RixnRkFBcUQ7SUFDckQsbUZBQXdEO0lBQ3hELDJGQUErRDtJQXFCL0Q7OztPQUdHO0lBQ0gsTUFBYSwwQkFBMEI7UUFHckMsWUFBbUIsV0FBMkI7WUFBM0IsZ0JBQVcsR0FBWCxXQUFXLENBQWdCO1lBRjlDLHNCQUFpQixHQUF1QixFQUFFLENBQUM7UUFFTSxDQUFDO1FBRWxELFNBQVMsQ0FBQyxJQUFhO1lBQ3JCLElBQUksSUFBSSxDQUFDLElBQUksS0FBSyxFQUFFLENBQUMsVUFBVSxDQUFDLGdCQUFnQixFQUFFO2dCQUNoRCxJQUFJLENBQUMscUJBQXFCLENBQUMsSUFBMkIsQ0FBQyxDQUFDO2FBQ3pEO1lBRUQsRUFBRSxDQUFDLFlBQVksQ0FBQyxJQUFJLEVBQUUsQ0FBQyxDQUFDLEVBQUUsQ0FBQyxJQUFJLENBQUMsU0FBUyxDQUFDLENBQUMsQ0FBQyxDQUFDLENBQUM7UUFDaEQsQ0FBQztRQUVPLHFCQUFxQixDQUFDLElBQXlCO1lBQ3JELElBQUksQ0FBQyxJQUFJLENBQUMsVUFBVSxJQUFJLENBQUMsSUFBSSxDQUFDLFVBQVUsQ0FBQyxNQUFNLEVBQUU7Z0JBQy9DLE9BQU87YUFDUjtZQUVELE1BQU0sWUFBWSxHQUFHLG9DQUFvQixDQUFDLElBQUksQ0FBQyxXQUFXLEVBQUUsSUFBSSxDQUFDLFVBQVUsQ0FBQyxDQUFDO1lBQzdFLE1BQU0sa0JBQWtCLEdBQUcsWUFBWSxDQUFDLElBQUksQ0FBQyxHQUFHLENBQUMsRUFBRSxDQUFD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QkFDMUYsQ0FBQyxDQUFDO2lCQUNKO1lBQ0gsQ0FBQyxDQUFDLENBQUM7UUFDTCxDQUFDO0tBQ0Y7SUE1RkQsZ0VBNEZDIiwic291cmNlc0NvbnRlbnQiOlsiLyoqXG4gKiBAbGljZW5zZVxuICogQ29weXJpZ2h0IEdvb2dsZSBJbmMuIEFsbCBSaWdodHMgUmVzZXJ2ZWQuXG4gKlxuICogVXNlIG9mIHRoaXMgc291cmNlIGNvZGUgaXMgZ292ZXJuZWQgYnkgYW4gTUlULXN0eWxlIGxpY2Vuc2UgdGhhdCBjYW4gYmVcbiAqIGZvdW5kIGluIHRoZSBMSUNFTlNFIGZpbGUgYXQgaHR0cHM6Ly9hbmd1bGFyLmlvL2xpY2Vuc2VcbiAqL1xuXG5pbXBvcnQge2V4aXN0c1N5bmMsIHJlYWRGaWxlU3luY30gZnJvbSAnZnMnO1xuaW1wb3J0IHtkaXJuYW1lLCByZXNvbHZlfSBmcm9tICdwYXRoJztcbmltcG9ydCAqIGFzIHRzIGZyb20gJ3R5cGVzY3JpcHQnO1xuXG5pbXBvcnQge2NvbXB1dGVMaW5lU3RhcnRzTWFwLCBnZXRMaW5lQW5kQ2hhcmFjdGVyRnJvbVBvc2l0aW9ufSBmcm9tICcuL2xpbmVfbWFwcGluZ3MnO1xuaW1wb3J0IHtnZXRBbmd1bGFyRGVjb3JhdG9yc30gZnJvbSAnLi9uZ19kZWNvcmF0b3JzJztcbmltcG9ydCB7dW53cmFwRXhwcmVzc2lvbn0gZnJvbSAnLi90eXBlc2NyaXB0L2Z1bmN0aW9ucyc7XG5pbXBvcnQge2dldFByb3BlcnR5TmFtZVRleHR9IGZyb20gJy4vdHlwZXNjcmlwdC9wcm9wZXJ0eV9uYW1lJztcblxuZXhwb3J0IGludGVyZmFjZSBSZXNvbHZlZFRlbXBsYXRlIHtcbiAgLyoqIENsYXNzIGRlY2xhcmF0aW9uIHRoYXQgY29udGFpbnMgdGhpcyB0ZW1wbGF0ZS4gKi9cbiAgY29udGFpbmVyOiB0cy5DbGFzc0RlY2xhcmF0aW9uO1xuICAvKiogRmlsZSBjb250ZW50IG9mIHRoZSBnaXZlbiB0ZW1wbGF0ZS4gKi9cbiAgY29udGVudDogc3RyaW5nO1xuICAvKiogU3RhcnQgb2Zmc2V0IG9mIHRoZSB0ZW1wbGF0ZSBjb250ZW50IChlLmcuIGluIHRoZSBpbmxpbmUgc291cmNlIGZpbGUpICovXG4gIHN0YXJ0OiBudW1iZXI7XG4gIC8qKiBXaGV0aGVyIHRoZSBnaXZlbiB0ZW1wbGF0ZSBpcyBpbmxpbmUgb3Igbm90LiAqL1xuICBpbmxpbmU6IGJvb2xlYW47XG4gIC8qKiBQYXRoIHRvIHRoZSBmaWxlIHRoYXQgY29udGFpbnMgdGhpcyB0ZW1wbGF0ZS4gKi9cbiAgZmlsZVBhdGg6IHN0cmluZztcbiAgLyoqXG4gICAqIEdldHMgdGhlIGNoYXJhY3RlciBhbmQgbGluZSBvZiBhIGdpdmVuIHBvc2l0aW9uIGluZGV4IGluIHRoZSB0ZW1wbGF0ZS5cbiAgICogSWYgdGhlIHRlbXBsYXRlIGlzIGRlY2xhcmVkIGlubGluZSB3aXRoaW4gYSBUeXBlU2NyaXB0IHNvdXJjZSBmaWxlLCB0aGUgbGluZSBhbmRcbiAgICogY2hhcmFjdGVyIGFyZSBiYXNlZCBvbiB0aGUgZnVsbCBzb3VyY2UgZmlsZSBjb250ZW50LlxuICAgKi9cbiAgZ2V0Q2hhcmFjdGVyQW5kTGluZU9mUG9zaXRpb246IChwb3M6IG51bWJlcikgPT4geyBjaGFyYWN0ZXI6IG51bWJlciwgbGluZTogbnVtYmVyIH07XG59XG5cbi8qKlxuICogVmlzaXRvciB0aGF0IGNhbiBiZSB1c2VkIHRvIGRldGVybWluZSBBbmd1bGFyIHRlbXBsYXRlcyByZWZlcmVuY2VkIHdpdGhpbiBnaXZlblxuICogVHlwZVNjcmlwdCBzb3VyY2UgZmlsZXMgKGlubGluZSB0ZW1wbGF0ZXMgb3IgZXh0ZXJuYWwgcmVmZXJlbmNlZCB0ZW1wbGF0ZXMpXG4gKi9cbmV4cG9ydCBjbGFzcyBOZ0NvbXBvbmVudFRlbXBsYXRlVmlzaXRvciB7XG4gIHJlc29sdmVkVGVtcGxhdGVzOiBSZXNvbHZlZFRlbXBsYXRlW10gPSBbXTtcblxuICBjb25zdHJ1Y3RvcihwdWJsaWMgdHlwZUNoZWNrZXI6IHRzLlR5cGVDaGVja2VyKSB7fVxuXG4gIHZpc2l0Tm9kZShub2RlOiB0cy5Ob2RlKSB7XG4gICAgaWYgKG5vZGUua2luZCA9PT0gdHMuU3ludGF4S2luZC5DbGFzc0RlY2xhcmF0aW9uKSB7XG4gICAgICB0aGlzLnZpc2l0Q2xhc3NEZWNsYXJhdGlvbihub2RlIGFzIHRzLkNsYXNzRGVjbGFyYXRpb24pO1xuICAgIH1cblxuICAgIHRzLmZvckVhY2hDaGlsZChub2RlLCBuID0+IHRoaXMudmlzaXROb2RlKG4pKTtcbiAgfVxuXG4gIHByaXZhdGUgdmlzaXRDbGFzc0RlY2xhcmF0aW9uKG5vZGU6IHRzLkNsYXNzRGVjbGFyYXRpb24pIHtcbiAgICBpZiAoIW5vZGUuZGVjb3JhdG9ycyB8fCAhbm9kZS5kZWNvcmF0b3JzLmxlbmd0aCkge1xuICAgICAgcmV0dXJuO1xuICAgIH1cblxuICAgIGNvbnN0IG5nRGVjb3JhdG9ycyA9IGdldEFuZ3VsYXJEZWNvcmF0b3JzKHRoaXMudHlwZUNoZWNrZXIsIG5vZGUuZGVjb3JhdG9ycyk7XG4gICAgY29uc3QgY29tcG9uZW50RGVjb3JhdG9yID0gbmdEZWNvcmF0b3JzLmZpbmQoZGVjID0+IGRlYy5uYW1lID09PSAnQ29tcG9uZW50Jyk7XG5cbiAgICAvLyBJbiBjYXNlIG5vIFwiQENvbXBvbmVudFwiIGRlY29yYXRvciBjb3VsZCBiZSBmb3VuZCBvbiB0aGUgY3VycmVudCBjbGFzcywgc2tpcC5cbiAgICBpZiAoIWNvbXBvbmVudERlY29yYXRvcikge1xuICAgICAgcmV0dXJuO1xuICAgIH1cblxuICAgIGNvbnN0IGRlY29yYXRvckNhbGwgPSBjb21wb25lbnREZWNvcmF0b3Iubm9kZS5leHByZXNzaW9uO1xuXG4gICAgLy8gSW4gY2FzZSB0aGUgY29tcG9uZW50IGRlY29yYXRvciBjYWxsIGlzIG5vdCB2YWxpZCwgc2tpcCB0aGlzIGNsYXNzIGRlY2xhcmF0aW9uLlxuICAgIGlmIChkZWNvcmF0b3JDYWxsLmFyZ3VtZW50cy5sZW5ndGggIT09IDEpIHtcbiAgICAgIHJldHVybjtcbiAgICB9XG5cbiAgICBjb25zdCBjb21wb25lbnRNZXRhZGF0YSA9IHVud3JhcEV4cHJlc3Npb24oZGVjb3JhdG9yQ2FsbC5hcmd1bWVudHNbMF0pO1xuXG4gICAgLy8gRW5zdXJlIHRoYXQgdGhlIGNvbXBvbmVudCBtZXRhZGF0YSBpcyBhbiBvYmplY3QgbGl0ZXJhbCBleHByZXNzaW9uLlxuICAgIGlmICghdHMuaXNPYmplY3RMaXRlcmFsRXhwcmVzc2lvbihjb21wb25lbnRNZXRhZGF0YSkpIHtcbiAgICAgIHJldHVybjtcbiAgICB9XG5cbiAgICBjb25zdCBzb3VyY2VGaWxlID0gbm9kZS5nZXRTb3VyY2VGaWxlKCk7XG4gICAgY29uc3Qgc291cmNlRmlsZU5hbWUgPSBzb3VyY2VGaWxlLmZpbGVOYW1lO1xuXG4gICAgLy8gV2FsayB0aHJvdWdoIGFsbCBjb21wb25lbnQgbWV0YWRhdGEgcHJvcGVydGllcyBhbmQgZGV0ZXJtaW5lIHRoZSByZWZlcmVuY2VkXG4gICAgLy8gSFRNTCB0ZW1wbGF0ZXMgKGVpdGhlciBleHRlcm5hbCBvciBpbmxpbmUpXG4gICAgY29tcG9uZW50TWV0YWRhdGEucHJvcGVydGllcy5mb3JFYWNoKHByb3BlcnR5ID0+IHtcbiAgICAgIGlmICghdHMuaXNQcm9wZXJ0eUFzc2lnbm1lbnQocHJvcGVydHkpKSB7XG4gICAgICAgIHJldHVybjtcbiAgICAgIH1cblxuICAgICAgY29uc3QgcHJvcGVydHlOYW1lID0gZ2V0UHJvcGVydHlOYW1lVGV4dChwcm9wZXJ0eS5uYW1lKTtcblxuICAgICAgLy8gSW4gY2FzZSB0aGVyZSBpcyBhbiBpbmxpbmUgdGVtcGxhdGUgc3BlY2lmaWVkLCBlbnN1cmUgdGhhdCB0aGUgdmFsdWUgaXMgc3RhdGljYWxseVxuICAgICAgLy8gYW5hbHl6YWJsZSBieSBjaGVja2luZyBpZiB0aGUgaW5pdGlhbGl6ZXIgaXMgYSBzdHJpbmcgbGl0ZXJhbC1saWtlIG5vZGUuXG4gICAgICBpZiAocHJvcGVydHlOYW1lID09PSAndGVtcGxhdGUnICYmIHRzLmlzU3RyaW5nTGl0ZXJhbExpa2UocHJvcGVydHkuaW5pdGlhbGl6ZXIpKSB7XG4gICAgICAgIC8vIE5lZWQgdG8gYWRkIGFuIG9mZnNldCBvZiBvbmUgdG8gdGhlIHN0YXJ0IGJlY2F1c2UgdGhlIHRlbXBsYXRlIHF1b3RlcyBhcmVcbiAgICAgICAgLy8gbm90IHBhcnQgb2YgdGhlIHRlbXBsYXRlIGNvbnRlbnQuXG4gICAgICAgIGNvbnN0IHRlbXBsYXRlU3RhcnRJZHggPSBwcm9wZXJ0eS5pbml0aWFsaXplci5nZXRTdGFydCgpICsgMTtcbiAgICAgICAgY29uc3QgZmlsZVBhdGggPSByZXNvbHZlKHNvdXJjZUZpbGVOYW1lKTtcbiAgICAgICAgdGhpcy5yZXNvbHZlZFRlbXBsYXRlcy5wdXNoKHtcbiAgICAgICAgICBmaWxlUGF0aDogZmlsZVBhdGgsXG4gICAgICAgICAgY29udGFpbmVyOiBub2RlLFxuICAgICAgICAgIGNvbnRlbnQ6IHByb3BlcnR5LmluaXRpYWxpemVyLnRleHQsXG4gICAgICAgICAgaW5saW5lOiB0cnVlLFxuICAgICAgICAgIHN0YXJ0OiB0ZW1wbGF0ZVN0YXJ0SWR4LFxuICAgICAgICAgIGdldENoYXJhY3RlckFuZExpbmVPZlBvc2l0aW9uOlxuICAgICAgICAgICAgICBwb3MgPT4gdHMuZ2V0TGluZUFuZENoYXJhY3Rlck9mUG9zaXRpb24oc291cmNlRmlsZSwgcG9zICsgdGVtcGxhdGVTdGFydElkeClcbiAgICAgICAgfSk7XG4gICAgICB9XG4gICAgICBpZiAocHJvcGVydHlOYW1lID09PSAndGVtcGxhdGVVcmwnICYmIHRzLmlzU3RyaW5nTGl0ZXJhbExpa2UocHJvcGVydHkuaW5pdGlhbGl6ZXIpKSB7XG4gICAgICAgIGNvbnN0IHRlbXBsYXRlUGF0aCA9IHJlc29sdmUoZGlybmFtZShzb3VyY2VGaWxlTmFtZSksIHByb3BlcnR5LmluaXRpYWxpemVyLnRleHQpO1xuXG4gICAgICAgIC8vIEluIGNhc2UgdGhlIHRlbXBsYXRlIGRvZXMgbm90IGV4aXN0IGluIHRoZSBmaWxlIHN5c3RlbSwgc2tpcCB0aGlzXG4gICAgICAgIC8vIGV4dGVybmFsIHRlbXBsYXRlLlxuICAgICAgICBpZiAoIWV4aXN0c1N5bmModGVtcGxhdGVQYXRoKSkge1xuICAgICAgICAgIHJldHVybjtcbiAgICAgICAgfVxuXG4gICAgICAgIGNvbnN0IGZpbGVDb250ZW50ID0gcmVhZEZpbGVTeW5jKHRlbXBsYXRlUGF0aCwgJ3V0ZjgnKTtcbiAgICAgICAgY29uc3QgbGluZVN0YXJ0c01hcCA9IGNvbXB1dGVMaW5lU3RhcnRzTWFwKGZpbGVDb250ZW50KTtcblxuICAgICAgICB0aGlzLnJlc29sdmVkVGVtcGxhdGVzLnB1c2goe1xuICAgICAgICAgIGZpbGVQYXRoOiB0ZW1wbGF0ZVBhdGgsXG4gICAgICAgICAgY29udGFpbmVyOiBub2RlLFxuICAgICAgICAgIGNvbnRlbnQ6IGZpbGVDb250ZW50LFxuICAgICAgICAgIGlubGluZTogZmFsc2UsXG4gICAgICAgICAgc3RhcnQ6IDAsXG4gICAgICAgICAgZ2V0Q2hhcmFjdGVyQW5kTGluZU9mUG9zaXRpb246IHBvcyA9PiBnZXRMaW5lQW5kQ2hhcmFjdGVyRnJvbVBvc2l0aW9uKGxpbmVTdGFydHNNYXAsIHBvcyksXG4gICAgICAgIH0pO1xuICAgICAgfVxuICAgIH0pO1xuICB9XG59XG4iXX0=