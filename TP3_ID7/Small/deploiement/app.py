import joblib
import pandas as pd
from fastapi import FastAPI

modele = joblib.load("modele.joblib")
app = FastAPI()

@app.post("/predict")
def predict(personne: dict):
    ligne = pd.DataFrame([personne])[modele.feature_names_in_]
    return {"sleep_disorder": str(modele.predict(ligne)[0])}
