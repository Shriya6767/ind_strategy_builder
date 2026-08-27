from src.core.modules import BaseModel

class LoadDataRequest(BaseModel):
    start_date: str
    end_date: str
    symbol: str = "sensex"
