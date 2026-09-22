from src.core.modules import FastAPI, CORSMiddleware, GZipMiddleware, uvicorn, asynccontextmanager
from src.core.config import CORS_ORIGINS, Database
from src.api.route import router
from src.services.backtesting.load_data import DataLoader


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Load the market data ONCE per process before serving: every backtest
    # then slices its own dates from this resident frame (see DataStore),
    # and forked worker pools inherit it copy-on-write.
    DataLoader.preload_from_env()
    yield
    # Clean shutdown: hand the pooled PostgreSQL connections back.
    Database.close_all()


app = FastAPI(lifespan=lifespan)

app.include_router(router, prefix="/api")

app.add_middleware(
    CORSMiddleware,
    allow_origins=CORS_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Compresses any response body over 1 KB
app.add_middleware(GZipMiddleware, minimum_size=1000, compresslevel=6)

# if __name__ == "__main__":
#     uvicorn.run("src.main:app", host="127.0.0.1", port=8000, reload=True)
# uvicorn src.main:app --host 0.0.0.0 --port 8000