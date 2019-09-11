/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 * 
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import {Selector} from 'testcafe';
import {PanelCollapsible} from '../components/PanelCollapsible';
import {Table} from '../components/Table';
import {ace, enterAceText} from '../components/ace';

export class Paragraph extends PanelCollapsible {
    constructor(title) {
        super(title);

        this.topRightExecuteButton = this.body.find('query-actions-button').find('button').withExactText('Execute');
        this.bottomExecuteButton = this.body.find('.sql-controls').find('button').withExactText('Execute');
        this.resultsTable = new Table(this.body.find('.table'));
        this.queryField = ace(this.body);
        this.showQueryButton = this.body.find('a').withExactText('Show query');
        this.clearResultButton = this.body.find('i.fa.fa-eraser');
        this.showStacktraceButton = this.body.find('a').withExactText('Show more');
    }
    async enterQuery(text, options = {replace: false}) {
        return await enterAceText(this.queryField.with({timeout: 0}), text, options);
    }
}

const showQueryDialogSelector = Selector('.modal-header').withText('SQL query').parent('.modal');

export const showQueryDialog = {
    dialog: showQueryDialogSelector,
    body: showQueryDialogSelector.find('.modal-body'),
    footer: showQueryDialogSelector.find('.modal-footer'),
    okButton: showQueryDialogSelector.find('button').withExactText('Ok')
};

const confirmClearQueryDialogSelector = Selector('.modal-header').withText('Confirm').parent('.modal');

export const confirmClearQueryDialog = {
    dialog: confirmClearQueryDialogSelector,
    confirmButton: confirmClearQueryDialogSelector.find('button').withExactText('Confirm')
};

const showStacktraceDialogSelector = Selector('.modal-header').withText('Error details').parent('.modal');
const stacktraceDialogRootCause = showStacktraceDialogSelector.find('.stacktrace-viewer__cause');
const stacktraceDialogRootCauseLine = showStacktraceDialogSelector.find('.stacktrace-viewer__trace');

export const showStacktraceDialog = {
    dialog: showStacktraceDialogSelector,
    rootCause: stacktraceDialogRootCause,
    rootCauseMsg: stacktraceDialogRootCause.find('span'),
    rootCauseFirstStacktraceLine: stacktraceDialogRootCauseLine,
    causeWithoutStacktrace: showStacktraceDialogSelector.find('.stacktrace-viewer__cause').withText('Cause without stacktrace'),
    downloadLink: showStacktraceDialogSelector.find('span').withText('Full stacktrace is not available'),
    okButton: showStacktraceDialogSelector.find('button').withExactText('OK')
};
