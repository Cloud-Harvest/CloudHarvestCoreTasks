FROM python:3.12-bookworm as python

WORKDIR /src

ENV PIP_ROOT_USER_ACTION=ignore

COPY . .

RUN pip install poetry pytest \
    && poetry config virtualenvs.create false \
    && poetry install --no-interaction

RUN pytest tests/

ENTRYPOINT /bin/bash
