# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import os

import catboost
import pandas as pd

DATA_PATH = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    '../../../../../../../examples/src/main/resources/datasets/boston_housing_dataset.txt')
MODEL_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                          "../models/model_reg.cbm")
DATA_SAMPLE_PREDICT_PATH = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    '../../../../../../../examples/src/main/resources/datasets/boston_housing_dataset-catboost-expected-results'
    '.txt')


def main():
    # load dataset
    features = [
        f'f_{i}'
        for i in range(13)
    ]
    target = 'target'

    df = pd.read_csv(DATA_PATH, names=features + ['target'])

    # fit model
    model = catboost.CatBoost({
        'loss_function': 'RMSE',
        'verbose': False,
        'random_seed': 0
    })
    model.fit(df[features], df[target])
    model.save_model(MODEL_PATH)

    # predict on sample
    predicts = model.predict(df[features])
    pd.DataFrame({
        'x': predicts
    }).to_csv(DATA_SAMPLE_PREDICT_PATH, index=False, header=False)

    # predict on one sample
    print('Parameters:')
    r = df[:1][features].to_dict('records')
    for k, v in r[0].items():
        print(f'input.put("{k}", {v}d);')

    print('Expected predict:')
    print(model.predict(df[:1])[0])


if __name__ == '__main__':
    main()
