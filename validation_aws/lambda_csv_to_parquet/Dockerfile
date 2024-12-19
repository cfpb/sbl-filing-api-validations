FROM --platform=linux/amd64 public.ecr.aws/lambda/python:3.12

RUN pip install "poetry==1.8.4"

WORKDIR ${LAMBDA_TASK_ROOT}

COPY ../pyproject.toml ${LAMBDA_TASK_ROOT}
COPY ../poetry.lock ${LAMBDA_TASK_ROOT}
COPY ../src ./src

RUN poetry config virtualenvs.create false
RUN poetry install --no-dev

COPY lambda_function.py ${LAMBDA_TASK_ROOT}

# Pass the name of the function handler as an argument to the runtime
CMD [ "lambda_function.lambda_handler" ]