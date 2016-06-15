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
import webpack from 'webpack';
import autoprefixer from 'autoprefixer-core';
import progressPlugin from './plugins/progress';

//import  Manifest from 'manifest-revision-webpack-plugin';
import ExtractTextPlugin from 'extract-text-webpack-plugin';
import {srcDir, destDir, rootDir} from '../paths';

const NODE_ENV = process.env.NODE_ENV || 'production';
const IS_DEVELOPMENT = NODE_ENV === 'development';
const stylesLoader = 'css-loader?sourceMap!postcss-loader!sass-loader?outputStyle=expanded&sourceMap=true&sourceMapContents=true';

export default () => {
    const NODE_MODULES_PATH = path.resolve('node_modules');

    const webpackConfig = {
        node: {
            fs: 'empty'
        },
        // Entry points.
        entry: {
            polyfill: 'babel-polyfill',
            app: path.join(srcDir, 'index.js'),
            vendor: path.join(srcDir, 'vendor.js')
        },

        // Output system.
        output: {
            path: destDir,
            publicPath: './',
            filename: '[name].js'
        },

        // Resolves modules.
        resolve: {
            extensions: ['', '.js'],
            root: [rootDir],
            modulesDirectories: [NODE_MODULES_PATH, './'],
            alias: {
            }
        },

        // Modules resolvers.
        module: {
            noParse: [],
            preLoaders: [
                {
                    test: /\.js$/,
                    exclude: [NODE_MODULES_PATH],
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
                        `jade-html-loader`
                    ]
                },
                {
                    test: /\.js$/,
                    loaders: ['baggage-loader?[file].html&[file].css']
                },
                {
                    test: /\.js$/,
                    exclude: [NODE_MODULES_PATH],
                    loaders: ['ng-annotate-loader']
                },
                {
                    test: /\.js$/,
                    exclude: [NODE_MODULES_PATH],
                    loader: 'babel-loader',
                    query: {
                        cacheDirectory: true,
                        plugins: ['transform-runtime', 'add-module-exports'],
                        presets: ['angular']

                    }
                },
                {
                    test: /\.css$/,
                    loaders: ['style-loader', 'css-loader?sourceMap', 'postcss-loader']
                },
                {
                    test: /\.(scss|sass)$/,
                    loader: IS_DEVELOPMENT ? `style-loader!${stylesLoader}` : ExtractTextPlugin.extract('style-loader', stylesLoader)
                },
                {
                    test: /\.(woff2|woff|ttf|eot|svg)?(\?v=[0-9]\.[0-9]\.[0-9])?$/,
                    loaders: [
                        'url-loader?name=fonts/[name]_[hash].[ext]'
                    ]
                },
                {
                    test: /\.(jpe?g|png|gif)$/i,
                    loaders: ['url-loader?name=images/[name]_[hash].[ext]']
                },
                {
                    test: require.resolve("jquery"),
                    loaders: [
                        "expose-loader?$",
                        "expose-loader?jQuery"
                    ]
                },
                {
                    test: require.resolve("nvd3"),
                    loaders: [
                        "expose-loader?nv"
                    ]
                }
            ]
        },

        // Postcss configuration.
        postcss: [autoprefixer({browsers: ['last 2 versions']})],

        // ESLint loader configuration.
        eslint: {
            failOnWarning: false,
            failOnError: false
        },

        // Load plugins.
        plugins: [
            new webpack.ProvidePlugin({
                $: 'jquery',
                jQuery: 'jquery',
                _: 'lodash',
                nv: 'nvd3'
            }),
            new webpack.DefinePlugin({'NODE_ENV': JSON.stringify(NODE_ENV)}),
            // new webpack.NoErrorsPlugin(),
            new webpack.optimize.AggressiveMergingPlugin({moveToParents: true}),
            new webpack.optimize.CommonsChunkPlugin({
                name: 'common',
                async: true,
                children: true,
                minChunks: Infinity
            }),
            // new Manifest(path.join(_path + '/config', 'manifest.json'), {
            //     rootAssetPath: rootAssetPath,
            //     ignorePaths: ['.DS_Store']
            // }),
            new ExtractTextPlugin('assets/css/[name]' + (IS_DEVELOPMENT ? '' : '.[chunkhash]') + '.css', {allChunks: true}),
            progressPlugin
        ]
    };

    return webpackConfig;
};
