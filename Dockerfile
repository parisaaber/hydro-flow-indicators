ARG PYTHON_RUNTIME=3.12

FROM public.ecr.aws/lambda/python:${PYTHON_RUNTIME} AS deps

ENV PREFIX=$LAMBDA_TASK_ROOT
ENV PYTHONPATH=$PREFIX/py PATH=$PREFIX/py/bin:$PATH
ENV PIP_NO_CACHE_DIR=1 PIP_DISABLE_PIP_VERSION_CHECK=1

RUN mkdir -p $PREFIX/py \
 && python -m pip install -U pip

RUN pip install -t $PREFIX/py \
      pandas \
      numpy \
      duckdb \
      scipy \
      "fastapi[all]" \
      pydantic \
      requests \
      pyarrow \
      geopandas \
      pyproj \
      shapely

FROM deps AS build
WORKDIR /tmp/pkg

COPY README.md setup.cfg pyproject.toml* ./
COPY src ./src

RUN python -m pip wheel . -w /tmp/wheels \
 && pip install --no-deps /tmp/wheels/*.whl -t $PREFIX/py \
 && python -m compileall -q $PREFIX/py \
 && rm -rf /tmp/wheels

FROM public.ecr.aws/lambda/python:${PYTHON_RUNTIME}

ENV PREFIX=$LAMBDA_TASK_ROOT
ENV PYTHONPATH=$PREFIX/py PATH=$PREFIX/py/bin:$PATH

RUN dnf install -y \
      libgomp \
      libstdc++ \
  && dnf clean all && rm -rf /var/cache/dnf

COPY --from=build $PREFIX/py $PREFIX/py
