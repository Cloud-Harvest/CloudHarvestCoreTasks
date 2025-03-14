FROM python:3.13-bookworm as python

WORKDIR /src

ENV PIP_ROOT_USER_ACTION=ignore

COPY . .

RUN pip install setuptools \
    && python -m pip install -r requirements.txt \
    && pytest tests/

ENTRYPOINT /bin/bash
