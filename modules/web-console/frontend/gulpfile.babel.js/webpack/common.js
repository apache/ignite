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

import ProgressBarPlugin from 'progress-bar-webpack-plugin';
import eslintFormatter from 'eslint-friendly-formatter';

import HtmlWebpackPlugin from 'html-webpack-plugin';

import ExtractTextPlugin from 'extract-text-webpack-plugin';

import {srcDir, destDir, rootDir, igniteModulesDir} from '../paths';

const viewsDir = path.resolve('views');
const imagesDir = path.resolve('public/images');
const iconsDir = path.resolve('public/images/icons');

const NODE_ENV = process.env.NODE_ENV || 'production';
const development = NODE_ENV === 'development';
const node_modules_path = path.resolve('node_modules');

let favicon = 'build/ignite_modules/favicon.ico';

try {
    fs.accessSync(path.join(igniteModulesDir, 'favicon.ico'), fs.F_OK);
} catch (ignore) {
    favicon = 'build/favicon.ico';
}

export default {
    cache: true,
    node: {
        fs: 'empty'
    },
    // Entry points.
    entry: {
        polyfill: 'babel-polyfill',
        vendor: path.join(srcDir, 'vendor.js'),
        app: path.join(srcDir, 'app.js')
    },

    // Output system.
    output: {
        path: destDir,
        filename: '[name].js'
    },

    // Resolves modules.
    resolve: {
        extensions: [
            '.js'
        ],
        modules: [
            srcDir,
            rootDir,
            node_modules_path
        ],
        // A list of module source folders.
        alias: {
            app: srcDir,
            views: viewsDir,
            images: imagesDir
        }
    },

    // Resolve loader use postfix.
    resolveLoader: {
        moduleExtensions: ['-loader']
    },

    module: {
        rules: [
            {
                test: /\.json$/,
                loader: 'json'
            },

            // Exclude tpl.pug files to import in bundle.
            {
                test: /^(?:(?!tpl\.pug$).)*\.pug$/, // TODO: check this regexp for correct.
                loader: `pug-html?basedir=${rootDir}`
            },

            // Render .tpl.pug files to assets folder.
            {
                test: /\.tpl\.pug$/,
                use: [
                    'file?exports=false&name=assets/templates/[name].[hash].html',
                    `pug-html?exports=false&basedir=${rootDir}`
                ]
            },
            {
                test: /\.js$/,
                enforce: 'pre',
                exclude: [node_modules_path],
                use: [{
                    loader: 'eslint',
                    options: {
                        failOnWarning: false,
                        failOnError: false,
                        formatter: eslintFormatter
                    }
                }]
            },
            {
                test: /\.js$/,
                exclude: [node_modules_path],
                use: [{
                    loader: 'babel',
                    options: {
                        cacheDirectory: true,
                        plugins: [
                            'transform-runtime',
                            'add-module-exports'
                        ],
                        presets: ['angular']
                    }
                }]
            },
            {
                test: /\.css$/,
                use: development ? ['style', 'css'] : ExtractTextPlugin.extract({
                    fallback: 'style',
                    use: ['css']
                })
            },
            {
                test: /\.scss$/,
                use: development ? ['style', 'css', 'sass'] : ExtractTextPlugin.extract({
                    fallback: 'style-loader',
                    use: ['css', 'sass']
                })
            },
            {
                test: /\.(ttf|eot|svg|woff(2)?)(\?v=[\d.]+)?(\?[a-z0-9#-]+)?$/,
                exclude: [iconsDir],
                loader: 'file?name=assets/fonts/[name].[ext]'
            },
            {
                test: /.*\.svg$/,
                include: [iconsDir],
                use: ['svg-sprite-loader']
            },
            {
                test: /\.(jpe?g|png|gif)$/i,
                loader: 'file?name=assets/images/[name]_[hash].[ext]'
            },
            {
                test: require.resolve('jquery'),
                use: [
                    'expose-loader?$',
                    'expose-loader?jQuery'
                ]
            },
            {
                test: require.resolve('nvd3'),
                use: ['expose-loader?nv']
            }
        ]
    },

    // Load plugins.
    plugins: [
        new webpack.LoaderOptionsPlugin({
            options: {
                pug: {
                    basedir: rootDir
                },
                target: 'web'
            }
        }),
        new webpack.ProvidePlugin({
            $: 'jquery',
            jQuery: 'jquery',
            _: 'lodash',
            nv: 'nvd3'
        }),
        new webpack.DefinePlugin({NODE_ENV: JSON.stringify(NODE_ENV)}),
        new webpack.optimize.CommonsChunkPlugin({name: 'vendor'}),
        new webpack.optimize.AggressiveMergingPlugin({moveToParents: true}),
        new HtmlWebpackPlugin({
            template: './views/index.pug',
            favicon
        }),
        new ExtractTextPlugin({filename: 'assets/css/[name].css', allChunks: true}),
        new ProgressBarPlugin()
    ]
};
