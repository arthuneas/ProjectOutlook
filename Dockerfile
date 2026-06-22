FROM python:3.10-slim

WORKDIR /app

# Instala dependências
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copia código-fonte
COPY src/ ./src/

# Cria pasta compartilhada
RUN mkdir -p /app/shared_folder

# Flag -u desabilita buffering do stdout (logs em tempo real)
CMD ["python", "-u", "src/main.py"]
