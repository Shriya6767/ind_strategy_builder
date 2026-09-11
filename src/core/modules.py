from fastapi import FastAPI, APIRouter, HTTPException, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
import orjson

class ORJSONResponse(Response):
    """orjson-serialized JSON response, returned directly from heavy routes
    (fastapi.responses.ORJSONResponse is deprecated in FastAPI 0.139+; its
    suggested replacement -- Pydantic response models -- would re-validate
    our already-sanitized dict payloads, which is exactly the overhead
    returning a Response directly avoids)."""

    media_type = "application/json"

    def render(self, content) -> bytes:
        return orjson.dumps(content)

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