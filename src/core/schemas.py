from src.core.modules import BaseModel

class LoadDataRequest(BaseModel):
    start_date: str          
    end_date: str            
    dte_type: str = "0dte"   
    symbol: str = "SPXW"  