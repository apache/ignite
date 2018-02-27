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

import path from 'path';
import fs from 'fs';
import webpack from 'webpack';
import autoprefixer from 'autoprefixer-core';
import jade from 'jade';
import ProgressBarPlugin from 'progress-bar-webpack-plugin';
import eslintFormatter from 'eslint-friendly-formatter';

import ExtractTextPlugin from 'extract-text-webpack-plugin';
import HtmlWebpackPlugin from 'html-webpack-plugin';

import {srcDir, destDir, rootDir, igniteModulesDir} from '../paths';

const NODE_ENV = process.env.NODE_ENV || 'production';
const development = NODE_ENV === 'development';
const node_modules_path = path.resolve('node_modules');
const cssLoader = 'css-loader?sourceMap!postcss-loader';
const stylesLoader = cssLoader + '!sass-loader?outputStyle=expanded&sourceMap=true&sourceMapContents=true';

let favicon = 'build/ignite_modules/favicon.ico';

try {
    fs.accessSync(path.join(igniteModulesDir, 'favicon.ico'), fs.F_OK);
} catch (ignore) {
    favicon = 'build/favicon.ico';
}

export default () => {
    const assetsLoader = 'file-loader';

    return {
        cache: true,
        node: {
            fs: 'empty'
        },

        // Entry points.
        entry: {
            polyfill: 'babel-polyfill',
            app: path.join(srcDir, 'app.js'),
            vendor: path.join(srcDir, 'vendor.js')
        },

        // Output system.
        output: {
            path: destDir,
            filename: '[name].js'
        },

        // Resolves modules.
        resolve: {
            extensions: [
                '',
                '.js'
            ],
            root: [rootDir],
            modulesDirectories: [
                node_modules_path,
                './'
            ]
        },
        jade: {
            basedir: rootDir,
            locals: {}
        },
        // Modules resolvers.
        /* global require */
        module: {
            noParse: [],
            preLoaders: [
                {
                    test: /\.js$/,
                    exclude: [node_modules_path],
                    loader: 'eslint-loader'
                }
            ],
            loaders: [
                {
                    test: /\.json$/,
                    loader: 'json-loader'
                },
                {
                    test: /\.jade$/,
                    loaders: [
                        `ngtemplate-loader?relativeTo=${rootDir}`,
                        'html-loader?attrs[]=img:src&attrs[]=img:data-src',
                        'jade-html-loader'
                    ]
                },
                {
                    test: /\.js$/,
                    exclude: [node_modules_path],
                    loader: 'babel-loader',
                    query: {
                        cacheDirectory: true,
                        plugins: [
                            'transform-runtime',
                            'add-module-exports'
                        ],
                        presets: ['angular']

                    }
                },
                {
                    test: /\.css$/,
                    loader: development ? `style-loader!${cssLoader}` : ExtractTextPlugin.extract('style-loader', cssLoader)
                },
                {
                    test: /\.(scss|sass)$/,
                    loader: development ? `style-loader!${stylesLoader}` : ExtractTextPlugin.extract('style-loader', stylesLoader)
                },
                {
                    test: /\.(ttf|eot|svg|woff(2)?)(\?v=[\d.]+)?(\?[a-z0-9#-]+)?$/,
                    loaders: [`${assetsLoader}?name=assets/fonts/[name].[ext]`]
                },
                {
                    test: /\.(jpe?g|png|gif)$/i,
                    loaders: [`${assetsLoader}?name=assets/images/[name]_[hash].[ext]`]
                },
                {
                    test: require.resolve('jquery'),
                    loaders: [
                        'expose-loader?$',
                        'expose-loader?jQuery'
                    ]
                },
                {
                    test: require.resolve('nvd3'),
                    loaders: [
                        'expose-loader?nv'
                    ]
                }
            ]
        },

        // Postcss configuration.
        postcss: [autoprefixer({browsers: ['last 2 versions']})],

        // ESLint loader configuration.
        eslint: {
            failOnWarning: false,
            failOnError: false,
            formatter: eslintFormatter
        },

        // Load plugins.
        plugins: [
            new webpack.ProvidePlugin({
                $: 'jquery',
                jQuery: 'jquery',
                _: 'lodash',
                nv: 'nvd3'
            }),
            new webpack.DefinePlugin({NODE_ENV: JSON.stringify(NODE_ENV)}),
            // new webpack.NoErrorsPlugin(),
            new webpack.optimize.DedupePlugin(),
            new webpack.optimize.CommonsChunkPlugin({
                name: 'common',
                chunks: ['vendor', 'app']
            }),
            new webpack.optimize.AggressiveMergingPlugin({moveToParents: true}),
            new webpack.optimize.OccurenceOrderPlugin(),
            new ExtractTextPlugin('assets/css/[name]' + (development ? '' : '.[chunkhash]') + '.css', {allChunks: true}),
            new HtmlWebpackPlugin({
                filename: 'index.html',
                templateContent: () => {
                    return jade.renderFile(path.join(rootDir, 'views', 'index.jade'));
                },
                favicon
            }),
            new ProgressBarPlugin()
        ]
    };
};
