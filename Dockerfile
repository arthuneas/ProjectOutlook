FROM python:3.10-slim

WORKDIR /app

# Copia e instala dependências (se houver requirements.txt no futuro)
# COPY requirements.txt .
# RUN pip install -r requirements.txt

# Para a biblioteca watchdog
RUN pip install watchdog

COPY src/ ./src/

# Define a pasta compartilhada
RUN mkdir -p /app/shared_folder

# Comando padrão
CMD ["python", "-u", "src/main.py"]
