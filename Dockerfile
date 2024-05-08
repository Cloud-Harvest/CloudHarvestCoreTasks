FROM python:3.12-bookworm as python

WORKDIR /src

ENV PIP_ROOT_USER_ACTION=ignore

COPY . .

RUN pip install -r requirements.txt \
    && pip install poetry \
    && poetry config virtualenvs.create false \
    && poetry install --only main --no-interaction

ENTRYPOINT python CloudHarvestApi/wsgi.py
