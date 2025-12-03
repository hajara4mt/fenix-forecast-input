# app/main.py
from fastapi import FastAPI

from app.routes import building , deliverypoint ,invoice , usagedata , degreedays   # importe le router qu'on vient de créer

app = FastAPI(
    title="FenixForecast Test API",
    version="1.0.0",
)


@app.get("/statut")
def health():
    return {"status": "ok"}



# on "branche" le router building
app.include_router(building.router)
app.include_router(deliverypoint.router)
app.include_router(invoice.router) 
app.include_router(usagedata.router)   
app.include_router(degreedays.router)

