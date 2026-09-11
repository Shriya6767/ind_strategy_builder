from src.core.modules import FastAPI, CORSMiddleware, GZipMiddleware, uvicorn
from src.api.route import router

app = FastAPI()

app.include_router(router, prefix="/api")

# [
#         "http://localhost:5173",
#     ],

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Compresses any response body over 1 KB
app.add_middleware(GZipMiddleware, minimum_size=1000, compresslevel=6)

# if __name__ == "__main__":
#     uvicorn.run("src.main:app", host="127.0.0.1", port=8000, reload=True)

# uvicorn src.main:app --host 0.0.0.0 --port 8000