# app/main.py
from fastapi import FastAPI

from app.routes import building , deliverypoint ,invoice , usagedata , degreedays , resultats  , season , invoice_batch# importe le router qu'on vient de créer

app = FastAPI(
    title="FenixForecast Test API-INPUT",
    version="1.0.0",
)


@app.get("/")
def root():
    return {"message": "API Fenix Forecast -INPUT- -Préprod- en ligne "}


# on "branche" le router building
app.include_router(building.router)
app.include_router(deliverypoint.router)
app.include_router(invoice.router) 
app.include_router(usagedata.router)   
app.include_router(degreedays.router)
app.include_router(resultats.router)
app.include_router(invoice_batch.router) 
app.include_router(season.router)
