#!/bin/sh
source venv/bin/activate
cd generated
uvicorn api:app