# api_server.py

from fastapi import FastAPI, HTTPException
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from pydantic import BaseModel
from typing import List
import uuid

app = FastAPI()

# Database Setup
engine = create_engine('postgresql://postgres:yourpassword@localhost:5432/option_pricing')
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# Pydantic Models
class OptionPrice(BaseModel):
    option_id: uuid.UUID
    timestamp: str
    underlying_price: float
    strike_price: float
    volatility: float
    interest_rate: float
    option_price_call: float
    option_price_put: float
    pricing_method: str

# API Endpoints
@app.get("/options/", response_model=List[OptionPrice])
def get_option_prices(limit: int = 100):
    session = SessionLocal()
    try:
        result = session.execute("SELECT * FROM btc_option_prices ORDER BY timestamp DESC LIMIT :limit", {'limit': limit})
        options = [OptionPrice(**row) for row in result]
        return options
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        session.close()

@app.get("/options/{option_id}", response_model=OptionPrice)
def get_option_price(option_id: uuid.UUID):
    session = SessionLocal()
    try:
        result = session.execute("SELECT * FROM btc_option_prices WHERE option_id = :option_id", {'option_id': option_id}).fetchone()
        if result:
            return OptionPrice(**result)
        else:
            raise HTTPException(status_code=404, detail="Option not found")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        session.close()
