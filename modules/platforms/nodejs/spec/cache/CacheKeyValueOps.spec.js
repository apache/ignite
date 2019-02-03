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

'use strict';

require('jasmine-expect');

const TestingHelper = require('../TestingHelper');
const IgniteClient = require('apache-ignite-client');
const ObjectType = IgniteClient.ObjectType;
const CacheClient = IgniteClient.CacheClient;
const CacheEntry = IgniteClient.CacheEntry;
const CacheConfiguration = IgniteClient.CacheConfiguration;

const CACHE_NAME = '__test_cache';

describe('cache key value operations test suite >', () => {
    let igniteClient = null;
    let cache = null;

    beforeAll((done) => {
        Promise.resolve().
            then(async () => {
                await TestingHelper.init();
                igniteClient = TestingHelper.igniteClient;
                await testSuiteCleanup(done);
                await igniteClient.getOrCreateCache(CACHE_NAME);
                cache = igniteClient.getCache(CACHE_NAME).
                    setKeyType(ObjectType.PRIMITIVE_TYPE.INTEGER).
                    setValueType(ObjectType.PRIMITIVE_TYPE.INTEGER);
            }).
            then(done).
            catch(error => done.fail(error));
    }, TestingHelper.TIMEOUT);

    afterAll((done) => {
        Promise.resolve().
            then(async () => {
                await testSuiteCleanup(done);
                await TestingHelper.cleanUp();
            }).
            then(done).
            catch(error => done.fail(error));
    }, TestingHelper.TIMEOUT);

    it ('get', (done) => {
        Promise.resolve().
            then(async () => {
                try {
                    let value = await cache.get(1);
                    expect(value).toBe(null);
                    await cache.put(1, 2);
                    value = await cache.get(1);
                    expect(value).toBe(2);
                }
                finally {
                    await cache.removeAll();
                }
            }).
            then(done).
            catch(error => done.fail(error));
    });

    it ('get wrong args', (done) => {
        Promise.resolve().
            then(async () => {
                try {
                    const wrongArgs = [null, undefined];
                    for (let arg of wrongArgs) {
                        try {
                            let value = await cache.get(arg);
                            done.fail(`cache.get(${arg}) is allowed`);
                        }
                        catch (err) {
                            TestingHelper.checkIllegalArgumentError(err, done);
                        }
                    }
                }
                finally {
                    await cache.removeAll();
                }
            }).
            then(done).
            catch(error => done.fail(error));
    });

    it ('getAll', (done) => {
        Promise.resolve().
            then(async () => {
                try {
                    for (let i = 0; i < 5; i++) {
                        await cache.put(i, i * 2);
                    }
                    let entries = await cache.getAll([3, 4, 5, 6, 7]);
                    expect(entries.length).toBe(2, 'get all length is incorrect');
                    for (let entry of entries) {
                        expect(entry.getKey()).toBeWithinRange(3, 4);
                        expect(entry.getValue()).toBe(entry.getKey() * 2);
                    }
                    entries = await cache.getAll([6, 7, 8]);
                    expect(entries.length).toBe(0, 'get all length is incorrect');
                }
                finally {
                    await cache.removeAll();
                }
            }).
            then(done).
            catch(error => done.fail(error));
    });

    it ('getAll wrong args', (done) => {
        Promise.resolve().
            then(async () => {
                try {
                    const wrongArgs = [null, undefined, 12345, 'abc', []];
                    for (let arg of wrongArgs) {
                        try {
                            let value = await cache.getAll(arg);
                            done.fail(`cache.getAll(${arg}) is allowed`);
                        }
                        catch (err) {
                            TestingHelper.checkIllegalArgumentError(err, done);
                        }
                    }
                }
                finally {
                    await cache.removeAll();
                }
            }).
            then(done).
            catch(error => done.fail(error));
    });

    it ('put', (done) => {
        Promise.resolve().
            then(async () => {
                try {
                    let value = await cache.get(1);
                    expect(value).toBe(null);
                    await cache.put(1, 2);
                    value = await cache.get(1);
                    expect(value).toBe(2);
                    await cache.put(1, 4);
                    value = await cache.get(1);
                    expect(value).toBe(4);
                }
                finally {
                    await cache.removeAll();
                }
            }).
            then(done).
            catch(error => done.fail(error));
    });

    it ('put wrong args', (done) => {
        Promise.resolve().
            then(async () => {
                try {
                    const args = [null, undefined, 1];
                    for (let key of args) {
                        for (let value of args) {
                            if (key === 1 && value === 1) {
                                continue;
                            }
                            try {
                                await cache.put(key, value);
                                done.fail(`cache.put(${key}, ${value}) is allowed`);
                            }
                            catch (err) {
                                TestingHelper.checkIllegalArgumentError(err, done);
                            }
                        }
                    }
                }
                finally {
                    await cache.removeAll();
                }
            }).
            then(done).
            catch(error => done.fail(error));
    });

    it ('putAll', (done) => {
        Promise.resolve().
            then(async () => {
                try {
                    const cacheEntries = new Array();
                    for (let i = 0; i < 5; i++) {
                        cacheEntries.push(new CacheEntry(i, i * 2));
                    }
                    await cache.putAll(cacheEntries);
                    let entries = await cache.getAll([3, 4, 5, 6, 7]);
                    expect(entries.length).toBe(2, 'get all length is incorrect');
                    for (let entry of entries) {
                        expect(entry.getKey()).toBeWithinRange(3, 4);
                        expect(entry.getValue()).toBe(entry.getKey() * 2);
                    }
                    entries = await cache.getAll([-2, -1, 0, 1, 2, 3, 4, 5, 6, 7]);
                    expect(entries.length).toBe(5, 'get all length is incorrect');
                    for (let entry of entries) {
                        expect(entry.getKey()).toBeWithinRange(0, 5);
                        expect(entry.getValue()).toBe(entry.getKey() * 2);
                    }
                    entries = await cache.getAll([6, 7, 8]);
                    expect(entries.length).toBe(0, 'get all length is incorrect');
                }
                finally {
                    await cache.removeAll();
                }
            }).
            then(done).
            catch(error => done.fail(error));
    });

    it ('putAll wrong args', (done) => {
        Promise.resolve().
            then(async () => {
                try {
                    const wrongArgs = [null, undefined, 12345, 'abc', [], [new CacheConfiguration()]];
                    for (let arg of wrongArgs) {
                        try {
                            let value = await cache.putAll(arg);
                            done.fail(`cache.putAll(${arg}) is allowed`);
                        }
                        catch (err) {
                            TestingHelper.checkIllegalArgumentError(err, done);
                        }
                    }
                }
                finally {
                    await cache.removeAll();
                }
            }).
            then(done).
            catch(error => done.fail(error));
    });

    it ('containsKey', (done) => {
        Promise.resolve().
            then(async () => {
                try {
                    let result = await cache.containsKey(1);
                    expect(result).toBe(false);
                    await cache.put(1, 2);
                    result = await cache.containsKey(1);
                    expect(result).toBe(true);
                }
                finally {
                    await cache.removeAll();
                }
            }).
            then(done).
            catch(error => done.fail(error));
    });

    it ('containsKey wrong args', (done) => {
        Promise.resolve().
            then(async () => {
                try {
                    const wrongArgs = [null, undefined];
                    for (let arg of wrongArgs) {
                        try {
                            let value = await cache.get(arg);
                            done.fail(`cache.containsKey(${arg}) is allowed`);
                        }
                        catch (err) {
                            TestingHelper.checkIllegalArgumentError(err, done);
                        }
                    }
                }
                finally {
                    await cache.removeAll();
                }
            }).
            then(done).
            catch(error => done.fail(error));
    });

    it ('containsKeys', (done) => {
        Promise.resolve().
            then(async () => {
                try {
                    let result = await cache.containsKeys([1, 2, 3]);
                    expect(result).toBe(false);
                    await cache.putAll([new CacheEntry(1, 2), new CacheEntry(2, 4)]);
                    result = await cache.containsKeys([1, 2, 3]);
                    expect(result).toBe(false);
                    await cache.put(3, 6);
                    result = await cache.containsKeys([1, 2, 3]);
                    expect(result).toBe(true);
                }
                finally {
                    await cache.removeAll();
                }
            }).
            then(done).
            catch(error => done.fail(error));
    });

    it ('containsKeys wrong args', (done) => {
        Promise.resolve().
            then(async () => {
                try {
                    const wrongArgs = [null, undefined, [], 12345, 'abc'];
                    for (let arg of wrongArgs) {
                        try {
                            let value = await cache.containsKeys(arg);
                            done.fail(`cache.containsKeys(${arg}) is allowed`);
                        }
                        catch (err) {
                            TestingHelper.checkIllegalArgumentError(err, done);
                        }
                    }
                }
                finally {
                    await cache.removeAll();
                }
            }).
            then(done).
            catch(error => done.fail(error));
    });

    it ('getAndPut', (done) => {
        Promise.resolve().
            then(async () => {
                try {
                    let value = await cache.getAndPut(1, 2);
                    expect(value).toBe(null);
                    value = await cache.getAndPut(1, 4);
                    expect(value).toBe(2);
                    await cache.put(1, 6);
                    value = await cache.getAndPut(1, 8);
                    expect(value).toBe(6);
                    value = await cache.get(1);
                    expect(value).toBe(8);
                }
                finally {
                    await cache.removeAll();
                }
            }).
            then(done).
            catch(error => done.fail(error));
    });

    it ('getAndPut wrong args', (done) => {
        Promise.resolve().
            then(async () => {
                try {
                    const args = [null, undefined, 1];
                    for (let key of args) {
                        for (let value of args) {
                            if (key === 1 && value === 1) {
                                continue;
                            }
                            try {
                                await cache.getAndPut(key, value);
                                done.fail(`cache.getAndPut(${key}, ${value}) is allowed`);
                            }
                            catch (err) {
                                TestingHelper.checkIllegalArgumentError(err, done);
                            }
                        }
                    }
                }
                finally {
                    await cache.removeAll();
                }
            }).
            then(done).
            catch(error => done.fail(error));
    });

    it ('getAndReplace', (done) => {
        Promise.resolve().
            then(async () => {
                try {
                    let value = await cache.getAndReplace(1, 2);
                    expect(value).toBe(null);
                    await cache.put(1, 4);
                    value = await cache.getAndReplace(1, 6);
                    expect(value).toBe(4);
                    value = await cache.get(1);
                    expect(value).toBe(6);
                }
                finally {
                    await cache.removeAll();
                }
            }).
            then(done).
            catch(error => done.fail(error));
    });

    it ('getAndReplace wrong args', (done) => {
        Promise.resolve().
            then(async () => {
                try {
                    const args = [null, undefined, 1];
                    for (let key of args) {
                        for (let value of args) {
                            if (key === 1 && value === 1) {
                                continue;
                            }
                            try {
                                await cache.getAndReplace(key, value);
                                done.fail(`cache.getAndReplace(${key}, ${value}) is allowed`);
                            }
                            catch (err) {
                                TestingHelper.checkIllegalArgumentError(err, done);
                            }
                        }
                    }
                }
                finally {
                    await cache.removeAll();
                }
            }).
            then(done).
            catch(error => done.fail(error));
    });

    it ('getAndRemove', (done) => {
        Promise.resolve().
            then(async () => {
                try {
                    let value = await cache.getAndRemove(1);
                    expect(value).toBe(null);
                    await cache.put(1, 2);
                    value = await cache.getAndRemove(1);
                    expect(value).toBe(2);
                    value = await cache.get(1);
                    expect(value).toBe(null);
                }
                finally {
                    await cache.removeAll();
                }
            }).
            then(done).
            catch(error => done.fail(error));
    });

    it ('getAndRemove wrong args', (done) => {
        Promise.resolve().
            then(async () => {
                try {
                    const args = [null, undefined];
                    for (let key of args) {
                        try {
                            await cache.getAndRemove(key);
                            done.fail(`cache.getAndRemove(${key}) is allowed`);
                        }
                        catch (err) {
                            TestingHelper.checkIllegalArgumentError(err, done);
                        }
                    }
                }
                finally {
                    await cache.removeAll();
                }
            }).
            then(done).
            catch(error => done.fail(error));
    });

    it ('putIfAbsent', (done) => {
        Promise.resolve().
            then(async () => {
                try {
                    let result = await cache.putIfAbsent(1, 2);
                    expect(result).toBe(true);
                    let value = await cache.get(1);
                    expect(value).toBe(2);
                    result = await cache.putIfAbsent(1, 4);
                    expect(result).toBe(false);
                    value = await cache.get(1);
                    expect(value).toBe(2);
                }
                finally {
                    await cache.removeAll();
                }
            }).
            then(done).
            catch(error => done.fail(error));
    });

    it ('putIfAbsent wrong args', (done) => {
        Promise.resolve().
            then(async () => {
                try {
                    const args = [null, undefined, 1];
                    for (let key of args) {
                        for (let value of args) {
                            if (key === 1 && value === 1) {
                                continue;
                            }
                            try {
                                await cache.putIfAbsent(key, value);
                                done.fail(`cache.putIfAbsent(${key}, ${value}) is allowed`);
                            }
                            catch (err) {
                                TestingHelper.checkIllegalArgumentError(err, done);
                            }
                        }
                    }
                }
                finally {
                    await cache.removeAll();
                }
            }).
            then(done).
            catch(error => done.fail(error));
    });

    it ('getAndPutIfAbsent', (done) => {
        Promise.resolve().
            then(async () => {
                try {
                    let value = await cache.getAndPutIfAbsent(1, 2);
                    expect(value).toBe(null);
                    value = await cache.get(1);
                    expect(value).toBe(2);
                    value = await cache.getAndPutIfAbsent(1, 4);
                    expect(value).toBe(2);
                    value = await cache.get(1);
                    expect(value).toBe(2);
                }
                finally {
                    await cache.removeAll();
                }
            }).
            then(done).
            catch(error => done.fail(error));
    });

    it ('getAndPutIfAbsent wrong args', (done) => {
        Promise.resolve().
            then(async () => {
                try {
                    const args = [null, undefined, 1];
                    for (let key of args) {
                        for (let value of args) {
                            if (key === 1 && value === 1) {
                                continue;
                            }
                            try {
                                await cache.getAndPutIfAbsent(key, value);
                                done.fail(`cache.getAndPutIfAbsent(${key}, ${value}) is allowed`);
                            }
                            catch (err) {
                                TestingHelper.checkIllegalArgumentError(err, done);
                            }
                        }
                    }
                }
                finally {
                    await cache.removeAll();
                }
            }).
            then(done).
            catch(error => done.fail(error));
    });

    it ('replace', (done) => {
        Promise.resolve().
            then(async () => {
                try {
                    let result = await cache.replace(1, 2);
                    expect(result).toBe(false);
                    let value = await cache.get(1);
                    expect(value).toBe(null);
                    await cache.put(1, 1);
                    result = await cache.replace(1, 4);
                    expect(result).toBe(true);
                    value = await cache.get(1);
                    expect(value).toBe(4);
                }
                finally {
                    await cache.removeAll();
                }
            }).
            then(done).
            catch(error => done.fail(error));
    });

    it ('replace wrong args', (done) => {
        Promise.resolve().
            then(async () => {
                try {
                    const args = [null, undefined, 1];
                    for (let key of args) {
                        for (let value of args) {
                            if (key === 1 && value === 1) {
                                continue;
                            }
                            try {
                                await cache.replace(key, value);
                                done.fail(`cache.replace(${key}, ${value}) is allowed`);
                            }
                            catch (err) {
                                TestingHelper.checkIllegalArgumentError(err, done);
                            }
                        }
                    }
                }
                finally {
                    await cache.removeAll();
                }
            }).
            then(done).
            catch(error => done.fail(error));
    });

    it ('replaceIfEquals', (done) => {
        Promise.resolve().
            then(async () => {
                try {
                    let result = await cache.replaceIfEquals(1, 2, 3);
                    expect(result).toBe(false);
                    await cache.put(1, 4);
                    result = await cache.replaceIfEquals(1, 2, 3);
                    expect(result).toBe(false);
                    let value = await cache.get(1);
                    expect(value).toBe(4);
                    result = await cache.replaceIfEquals(1, 4, 3);
                    expect(result).toBe(true);
                    value = await cache.get(1);
                    expect(value).toBe(3);
                }
                finally {
                    await cache.removeAll();
                }
            }).
            then(done).
            catch(error => done.fail(error));
    });

    it ('replaceIfEquals wrong args', (done) => {
        Promise.resolve().
            then(async () => {
                try {
                    const args = [null, undefined, 1];
                    for (let key of args) {
                        for (let value of args) {
                            for (let newValue of args) {
                                if (key === 1 && value === 1 && newValue === 1) {
                                    continue;
                                }
                                try {
                                    await cache.replaceIfEquals(key, value, newValue);
                                    done.fail(`cache.replaceIfEquals(${key}, ${value}, ${newValue}) is allowed`);
                                }
                                catch (err) {
                                    TestingHelper.checkIllegalArgumentError(err, done);
                                }
                            }
                        }
                    }
                }
                finally {
                    await cache.removeAll();
                }
            }).
            then(done).
            catch(error => done.fail(error));
    });

    it ('clear', (done) => {
        Promise.resolve().
            then(async () => {
                try {
                    await cache.clear();
                    let result = await cache.getSize();
                    expect(result).toBe(0);
                    await cache.putAll([new CacheEntry(1, 2), new CacheEntry(2, 4), new CacheEntry(3, 6)]);
                    result = await cache.getSize();
                    expect(result).toBe(3);
                    await cache.clear();
                    result = await cache.getSize();
                    expect(result).toBe(0);
                    let entries = await cache.getAll([1, 2, 3]);
                    expect(entries.length).toBe(0, 'get all length is incorrect');
                }
                finally {
                    await cache.removeAll();
                }
            }).
            then(done).
            catch(error => done.fail(error));
    });

    it ('clearKeys', (done) => {
        Promise.resolve().
            then(async () => {
                try {
                    await cache.clearKeys([1, 2, 3]);
                    let result = await cache.getSize();
                    expect(result).toBe(0);
                    await cache.putAll([new CacheEntry(1, 2), new CacheEntry(2, 4), new CacheEntry(3, 6)]);
                    result = await cache.getSize();
                    expect(result).toBe(3);
                    await cache.clearKeys([1, 2, 7, 8]);
                    result = await cache.getSize();
                    expect(result).toBe(1);
                    let value = await cache.get(3);
                    expect(value).toBe(6);
                }
                finally {
                    await cache.removeAll();
                }
            }).
            then(done).
            catch(error => done.fail(error));
    });

    it ('clearKeys wrong args', (done) => {
        Promise.resolve().
            then(async () => {
                try {
                    const wrongArgs = [null, undefined, [], 12345, 'abc'];
                    for (let arg of wrongArgs) {
                        try {
                            let value = await cache.clearKeys(arg);
                            done.fail(`cache.clearKeys(${arg}) is allowed`);
                        }
                        catch (err) {
                            TestingHelper.checkIllegalArgumentError(err, done);
                        }
                    }
                }
                finally {
                    await cache.removeAll();
                }
            }).
            then(done).
            catch(error => done.fail(error));
    });

    it ('clearKey', (done) => {
        Promise.resolve().
            then(async () => {
                try {
                    await cache.clearKey(1);
                    let result = await cache.getSize();
                    expect(result).toBe(0);
                    await cache.putAll([new CacheEntry(1, 2), new CacheEntry(2, 4), new CacheEntry(3, 6)]);
                    result = await cache.getSize();
                    expect(result).toBe(3);
                    await cache.clearKey(1);
                    result = await cache.getSize();
                    expect(result).toBe(2);
                    let value = await cache.get(2);
                    expect(value).toBe(4);
                    value = await cache.get(3);
                    expect(value).toBe(6);
                }
                finally {
                    await cache.removeAll();
                }
            }).
            then(done).
            catch(error => done.fail(error));
    });

    it ('clearKey wrong args', (done) => {
        Promise.resolve().
            then(async () => {
                try {
                    const wrongArgs = [null, undefined];
                    for (let arg of wrongArgs) {
                        try {
                            let value = await cache.clearKey(arg);
                            done.fail(`cache.clearKey(${arg}) is allowed`);
                        }
                        catch (err) {
                            TestingHelper.checkIllegalArgumentError(err, done);
                        }
                    }
                }
                finally {
                    await cache.removeAll();
                }
            }).
            then(done).
            catch(error => done.fail(error));
    });

    it ('removeKey', (done) => {
        Promise.resolve().
            then(async () => {
                try {
                    let result = await cache.removeKey(1);
                    expect(result).toBe(false);
                    result = await cache.getSize();
                    expect(result).toBe(0);
                    await cache.putAll([new CacheEntry(1, 2), new CacheEntry(2, 4), new CacheEntry(3, 6)]);
                    result = await cache.getSize();
                    expect(result).toBe(3);
                    result = await cache.removeKey(1);
                    expect(result).toBe(true);
                    result = await cache.removeKey(1);
                    expect(result).toBe(false);
                    result = await cache.getSize();
                    expect(result).toBe(2);
                    let value = await cache.get(2);
                    expect(value).toBe(4);
                    value = await cache.get(3);
                    expect(value).toBe(6);
                }
                finally {
                    await cache.removeAll();
                }
            }).
            then(done).
            catch(error => done.fail(error));
    });

    it ('removeKey wrong args', (done) => {
        Promise.resolve().
            then(async () => {
                try {
                    const wrongArgs = [null, undefined];
                    for (let arg of wrongArgs) {
                        try {
                            let value = await cache.removeKey(arg);
                            done.fail(`cache.removeKey(${arg}) is allowed`);
                        }
                        catch (err) {
                            TestingHelper.checkIllegalArgumentError(err, done);
                        }
                    }
                }
                finally {
                    await cache.removeAll();
                }
            }).
            then(done).
            catch(error => done.fail(error));
    });

    it ('removeIfEquals', (done) => {
        Promise.resolve().
            then(async () => {
                try {
                    let result = await cache.removeIfEquals(1, 2);
                    expect(result).toBe(false);
                    await cache.put(1, 4);
                    result = await cache.removeIfEquals(1, 2);
                    expect(result).toBe(false);
                    result = await cache.removeIfEquals(1, 4);
                    expect(result).toBe(true);
                    let value = await cache.get(1);
                    expect(value).toBe(null);
                }
                finally {
                    await cache.removeAll();
                }
            }).
            then(done).
            catch(error => done.fail(error));
    });

    it ('removeIfEquals wrong args', (done) => {
        Promise.resolve().
            then(async () => {
                try {
                    const args = [null, undefined, 1];
                    for (let key of args) {
                        for (let value of args) {
                            if (key === 1 && value === 1) {
                                continue;
                            }
                            try {
                                await cache.removeIfEquals(key, value);
                                done.fail(`cache.removeIfEquals(${key}, ${value}) is allowed`);
                            }
                            catch (err) {
                                TestingHelper.checkIllegalArgumentError(err, done);
                            }
                        }
                    }
                }
                finally {
                    await cache.removeAll();
                }
            }).
            then(done).
            catch(error => done.fail(error));
    });

    it ('removeKeys', (done) => {
        Promise.resolve().
            then(async () => {
                try {
                    await cache.removeKeys([1, 2, 3]);
                    let result = await cache.getSize();
                    expect(result).toBe(0);
                    await cache.putAll([new CacheEntry(1, 2), new CacheEntry(2, 4), new CacheEntry(3, 6)]);
                    result = await cache.getSize();
                    expect(result).toBe(3);
                    await cache.removeKeys([1, 2, 7, 8]);
                    result = await cache.getSize();
                    expect(result).toBe(1);
                    let value = await cache.get(3);
                    expect(value).toBe(6);
                }
                finally {
                    await cache.removeAll();
                }
            }).
            then(done).
            catch(error => done.fail(error));
    });

    it ('removeKeys wrong args', (done) => {
        Promise.resolve().
            then(async () => {
                try {
                    const wrongArgs = [null, undefined, [], 12345, 'abc'];
                    for (let arg of wrongArgs) {
                        try {
                            let value = await cache.removeKeys(arg);
                            done.fail(`cache.removeKeys(${arg}) is allowed`);
                        }
                        catch (err) {
                            TestingHelper.checkIllegalArgumentError(err, done);
                        }
                    }
                }
                finally {
                    await cache.removeAll();
                }
            }).
            then(done).
            catch(error => done.fail(error));
    });

    it ('removeAll', (done) => {
        Promise.resolve().
            then(async () => {
                try {
                    await cache.removeAll();
                    let result = await cache.getSize();
                    expect(result).toBe(0);
                    await cache.putAll([new CacheEntry(1, 2), new CacheEntry(2, 4), new CacheEntry(3, 6)]);
                    result = await cache.getSize();
                    expect(result).toBe(3);
                    await cache.removeAll();
                    result = await cache.getSize();
                    expect(result).toBe(0);
                    let entries = await cache.getAll([1, 2, 3]);
                    expect(entries.length).toBe(0, 'get all length is incorrect');
                }
                finally {
                    await cache.removeAll();
                }
            }).
            then(done).
            catch(error => done.fail(error));
    });

    it ('getSize', (done) => {
        Promise.resolve().
            then(async () => {
                try {
                    let result = await cache.getSize();
                    expect(result).toBe(0);
                    await cache.putAll([new CacheEntry(1, 2), new CacheEntry(2, 4), new CacheEntry(3, 6)]);
                    result = await cache.getSize();
                    expect(result).toBe(3);
                    result = await cache.getSize(CacheClient.PEEK_MODE.ALL);
                    expect(result).toBe(3);
                    result = await cache.getSize(CacheClient.PEEK_MODE.ALL, CacheClient.PEEK_MODE.ALL);
                    expect(result).toBe(3);
                }
                finally {
                    await cache.removeAll();
                }
            }).
            then(done).
            catch(error => done.fail(error));
    });

    it ('getSize wrong args', (done) => {
        Promise.resolve().
            then(async () => {
                try {
                    const wrongArgs = [null, undefined, [], 12345, 'abc', [12345], ['abc', 'def']];
                    for (let arg of wrongArgs) {
                        try {
                            let value = await cache.getSize(arg);
                            done.fail(`cache.getSize(${arg}) is allowed`);
                        }
                        catch (err) {
                            TestingHelper.checkIllegalArgumentError(err, done);
                        }
                    }
                }
                finally {
                    await cache.removeAll();
                }
            }).
            then(done).
            catch(error => done.fail(error));
    });

    async function testSuiteCleanup(done) {
        await TestingHelper.destroyCache(CACHE_NAME, done);
    }
});
