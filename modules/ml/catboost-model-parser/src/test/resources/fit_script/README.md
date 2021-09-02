# Fit catboost regression / classification model

1. Install packages from `install.txt`
    ```
    pip install -r install.txt
    ```

2. Run `train_clf.py` and use:
    - Model file `model_clf.cbm`
    - Stdout `Parameters` & `Expected predict` for unit test
    - Test prediction in examples `amazon-employee-access-challenge-sample-expected-results.csv`

Full version of dataset (amazon-employee-access-challenge.csv) avaliable
at https://www.kaggle.com/c/amazon-employee-access-challenge/data

3. Run `train_reg.py` and use:
    - Model file `model_reg.cbm`
    - Stdout `Parameters` & `Expected predict` for unit test
    - Test prediction in examples `boston_housing_dataset-catboost-expected-results.txt`
