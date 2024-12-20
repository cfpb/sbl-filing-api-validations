FROM python:3.12-alpine

WORKDIR /usr/app

RUN pip install poetry

COPY poetry.lock pyproject.toml ./

RUN poetry config virtualenvs.create false
RUN poetry install --no-root

COPY ./src ./src

WORKDIR /usr/app/src

EXPOSE 8888

CMD ["uvicorn", "sbl_validation_service.main:app", "--workers", "13", "--host", "0.0.0.0", "--port", "8888", "--log-config", "log-config.yml"]
