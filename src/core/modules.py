from fastapi import FastAPI, APIRouter, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import uvicorn
import logging
import os
from logging.handlers import RotatingFileHandler
import pandas as pd
import numpy as np
from dotenv import load_dotenv
import psycopg2
from pydantic import BaseModel
import pyarrow.dataset as ds
from calendar import month_name
from datetime import datetime, timedelta, time as dt_time