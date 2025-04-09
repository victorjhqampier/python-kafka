FROM tiangolo/uvicorn-gunicorn-fastapi:python3.11
WORKDIR /axapp

RUN python -m pip install --upgrade pip
COPY src/ .
RUN pip install --no-cache-dir --upgrade -r requirements.txt

CMD ["uvicorn", "Interface.app:app", "--host", "0.0.0.0", "--port", "80", "--workers", "5"]