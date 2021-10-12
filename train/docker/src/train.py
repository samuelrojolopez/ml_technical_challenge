# coding: utf-8
import pandas as pd
from joblib import dump, load
from imblearn.over_sampling import SMOTE
from sklearn.model_selection import train_test_split

from sklearn.ensemble import RandomForestClassifier


if __name__ == "__main__":

    TRAINING_DATA_PATH = "/opt/ml/input/data/train/"
    MODEL_SAVE_DIR = "/opt/ml/model/"

    print("Reading training data...")
    training_data_file_paths = glob(
        TRAINING_DATA_PATH + "/**/*.parquet", recursive=True
    )

    print("Training data files:", training_data_file_paths)
    training_dfs = [
        pd.read_parquet(training_data_file_path)
        for training_data_file_path in training_data_file_paths
    ]
    training_df = pd.concat(training_dfs, ignore_index=True)

    print("All training data:")
    training_df.info(verbose=True, show_counts=True)
    del training_dfs
    print("Reading training data ✅")

    print("Fill Nans ... ")
    training_df.fillna(0, inplace=True)
    print("✅")

    print("Train Model ... ")
    Y = training_df['status']
    training_df.drop(['status'], axis=1, inplace=True)
    X = training_df

    # Using Synthetic Minority Over-Sampling Technique(SMOTE) to
    # overcome sample imbalance problem.
    Y = Y.astype('int')
    X_balance, Y_balance = SMOTE().fit_resample(X, Y)
    X_balance = pd.DataFrame(X_balance, columns=X.columns)
    X_train, X_test, y_train, y_test = train_test_split(X_balance, Y_balance,
                                                        stratify=Y_balance,
                                                        test_size=0.3,
                                                        random_state=123)

    model = RandomForestClassifier(n_estimators=5)

    model.fit(X_train, y_train)
    print("✅")

    print("Saving artifacts...")
    dump(model, MODEL_SAVE_DIR + 'model_risk.joblib')
    print("Artifact saved", "✅")

    print("SUCCESS")











