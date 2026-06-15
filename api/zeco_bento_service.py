import bentoml
from pydantic import BaseModel
from datetime import datetime


class PredictionInput(BaseModel):
    town: str
    vendor_id: str
    purchase_amount: float
    electricity_units: float
    outstanding_amount: float = 0.0


@bentoml.service
class ZECOEnergyPredictionService:

    @bentoml.api
    def predict_risk(self, data: PredictionInput) -> dict:
        amount = data.purchase_amount
        units = data.electricity_units
        outstanding = data.outstanding_amount

        if outstanding > 50000 or amount < 1000 or units < 2:
            risk = "HIGH"
            score = 0.85
        elif outstanding > 10000 or amount < 3000:
            risk = "MEDIUM"
            score = 0.55
        else:
            risk = "LOW"
            score = 0.15

        predicted_next_purchase = round(amount * (1.08 if risk == "LOW" else 0.85), 2)

        return {
            "project": "ZECO Intelligent Energy Analytics Platform",
            "risk_level": risk,
            "risk_score": score,
            "predicted_next_purchase": predicted_next_purchase,
            "town": data.town,
            "vendor_id": data.vendor_id,
            "prediction_time": datetime.utcnow().isoformat()
        }

    @bentoml.api
    def health(self) -> dict:
        return {
            "service": "ZECO BentoML Prediction API",
            "status": "healthy",
            "time": datetime.utcnow().isoformat()
        }