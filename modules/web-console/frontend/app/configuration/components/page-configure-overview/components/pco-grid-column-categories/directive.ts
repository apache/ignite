/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import isEqual from 'lodash/isEqual';
import map from 'lodash/map';
import uniqBy from 'lodash/uniqBy';
import headerTemplate from 'app/primitives/ui-grid-header/index.tpl.pug';
import {IGridColumn, IUiGridConstants} from 'ui-grid';

const visibilityChanged = (a, b) => {
    return !isEqual(map(a, 'visible'), map(b, 'visible'));
};

const notSelectionColumn = (cc: IGridColumn): boolean => cc.colDef.name !== 'selectionRowHeaderCol';

/**
 * Generates categories for uiGrid columns
 */
export default function directive(uiGridConstants: IUiGridConstants) {
    return {
        require: '^uiGrid',
        link: {
            pre(scope, el, attr, grid) {
                if (!grid.grid.options.enableColumnCategories)
                    return;

                grid.grid.api.core.registerColumnsProcessor((cp) => {
                    const oldCategories = grid.grid.options.categories;
                    const newCategories = uniqBy(cp.filter(notSelectionColumn).map(({colDef: cd}) => {
                        cd.categoryDisplayName = cd.categoryDisplayName || cd.displayName;
                        return {
                            name: cd.categoryDisplayName || cd.displayName,
                            enableHiding: cd.enableHiding,
                            visible: !!cd.visible
                        };
                    }), 'name');

                    if (visibilityChanged(oldCategories, newCategories)) {
                        grid.grid.options.categories = newCategories;
                        // If you don't call this, grid-column-selector won't apply calculated categories
                        grid.grid.callDataChangeCallbacks(uiGridConstants.dataChange.COLUMN);
                    }

                    return cp;
                });
                grid.grid.options.headerTemplate = headerTemplate;
            }
        }
    };
}

directive.$inject = ['uiGridConstants'];
