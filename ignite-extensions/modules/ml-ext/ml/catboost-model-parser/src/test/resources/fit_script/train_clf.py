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
import numpy as np
import pandas as pd

DATA_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "../datasets/amazon-employee-access-challenge.csv")
DATA_SAMPLE_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                "../datasets/amazon-employee-access-challenge-sample.csv")
DATA_SAMPLE_PREDICT_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                "../datasets/amazon-employee-access-challenge-sample-expected-results.csv")
MODEL_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                          "../models/model_clf.cbm")


def main():
    # load dataset
    df = pd.read_csv(DATA_PATH)

    target = 'ACTION'
    features = [
        column
        for column in df.columns
        if column != target
    ]

    # fit model
    model = catboost.CatBoost({
        'loss_function': 'Logloss',
        'verbose': False,
        'random_seed': 0
    })
    model.fit(df[features], df[target])
    model.save_model(MODEL_PATH)

    # predict on sample
    df_sample = pd.read_csv(DATA_SAMPLE_PATH)
    predicts = model.predict(df_sample)
    predicts = np.power(1 + np.exp(-predicts), -1)
    pd.DataFrame({'x': predicts}).to_csv(DATA_SAMPLE_PREDICT_PATH, index=False, header=False)

    # predict on one sample
    print('Parameters:')
    r = df_sample[:1].to_dict('records')
    for k, v in r[0].items():
        print(f'input.put("{k}", {v}.0);')

    print('Expected predict:')
    print(np.power(1 + np.exp(-model.predict(df_sample[:1])[0]), -1))

    # Use formula for classification probability = power(1 + exp(-predict), -1)
    # ref: https://github.com/catboost/benchmarks/blob/61d62512f751325a14dd885bb71f8c2dabf7e24b/quality_benchmarks/catboost_experiment.py


if __name__ == '__main__':
    main()
