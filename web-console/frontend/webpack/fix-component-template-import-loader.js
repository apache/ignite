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

/**
 * This is a workaround for the following issues:
 * https://github.com/TheLarkInn/angular2-template-loader/issues/86
 * https://github.com/webpack-contrib/raw-loader/issues/78
 */

const templateRegExp = /template:\s*require\(['"`].+['"`]\)/gm;

/**
 * @param {string} source
 * @param sourcemap
 */
module.exports = function(source, sourcemap) {
    this.cacheable && this.cacheable();

    const newSource = source.replace(templateRegExp, (match) => `${match}.default`);

    if (this.callback)
        this.callback(null, newSource, sourcemap);
    else
        return newSource;
};
