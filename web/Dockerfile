# Autor: Christiano Henrique Carrilho Lopes da Silva
# Código de pessoa: 1691353
# Matrícula: 262987
# Imagem da Web/API (#9): FastAPI servida pelo uvicorn em python:3.12.16-slim, versão fixada na #1.
# Copia só web/, sem o pacote do pipeline: o contrato entre os módulos é o esquema em db/.
# Roda com usuário sem privilégios; os testes vão na imagem para rodarem com as mesmas dependências.
FROM python:3.12.16-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app

COPY requirements.txt .
RUN pip install --root-user-action=ignore --no-deps -r requirements.txt \
    && pip check

COPY shopbrasil_web ./shopbrasil_web
COPY tests ./tests

RUN useradd --system --uid 10001 --no-create-home shopbrasil
USER shopbrasil

EXPOSE 8000

# Consulta a última data: confirma o processo e a conexão com a conta de leitura.
HEALTHCHECK --interval=10s --timeout=5s --start-period=10s --retries=6 \
    CMD ["python", "-c", "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8000/api/metricas/ultima-data', timeout=4)"]

CMD ["uvicorn", "shopbrasil_web.composicao:criar_app", "--factory", "--host", "0.0.0.0", "--port", "8000", "--no-access-log"]
