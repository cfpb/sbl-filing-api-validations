import logging

from fastapi import FastAPI

from sbl_validation_service.routers.endpoint import router as validation_router

log = logging.getLogger()

app = FastAPI()

app.include_router(validation_router, prefix="/v1/validator")
