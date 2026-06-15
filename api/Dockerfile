FROM python:3.12-slim

WORKDIR /app

RUN pip install --no-cache-dir bentoml pandas numpy

COPY zeco_bento_service.py /app/zeco_bento_service.py

EXPOSE 3001

CMD ["bentoml", "serve", "zeco_bento_service:ZECOEnergyPredictionService", "--host", "0.0.0.0", "--port", "3001"]