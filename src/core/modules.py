from fastapi import FastAPI, APIRouter, HTTPException, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
import orjson
import decimal


def _orjson_default(obj):
    """Fallback for the few types orjson does not serialize natively.
    psycopg2 returns NUMERIC columns (version_result metrics) as Decimal --
    FastAPI's default JSONResponse ran jsonable_encoder, which turned them
    into floats; returning a Response directly skips that step, so do it
    here. Anything else unknown becomes its str form rather than a 500."""
    if isinstance(obj, decimal.Decimal):
        return float(obj)
    return str(obj)


class ORJSONResponse(Response):
    """orjson-serialized JSON response, returned directly from heavy routes
    (fastapi.responses.ORJSONResponse is deprecated in FastAPI 0.139+; its
    suggested replacement -- Pydantic response models -- would re-validate
    our already-sanitized dict payloads, which is exactly the overhead
    returning a Response directly avoids)."""

    media_type = "application/json"

    def render(self, content) -> bytes:
        return orjson.dumps(content, default=_orjson_default)

import uvicorn
import logging
import os
import re
from logging.handlers import RotatingFileHandler
import pandas as pd
import numpy as np
from dotenv import load_dotenv
import psycopg2
from pydantic import BaseModel
import pyarrow.dataset as ds
from calendar import month_name
from datetime import datetime, date, timedelta, time as dt_time