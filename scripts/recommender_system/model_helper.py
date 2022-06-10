from pyspark.mllib.recommendation import ALS, MatrixFactorizationModel
import os, shutil

def train_ALS(ratings, rank, numIterations):
    model = ALS.train(ratings, rank, numIterations)
    return model

def evaluate_ALS(model, ratings):
    testdata = ratings.map(lambda p: (p[0], p[1]))
    predictions = model.predictAll(testdata).map(lambda r: ((r[0], r[1]), r[2]))
    ratesAndPreds = ratings.map(lambda r: ((r[0], r[1]), r[2])).join(predictions)
    MSE = ratesAndPreds.map(lambda r: (r[1][0] - r[1][1])**2).mean()
    print("Mean Squared Error = %.3f" % MSE)

def save_model(model, path, sc):
    if os.path.exists(path) and os.path.isdir(path):
        shutil.rmtree(path)
    model.save(sc, path)

def load_model(sc, model_path):
    return MatrixFactorizationModel.load(sc, model_path)