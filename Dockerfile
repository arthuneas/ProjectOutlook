FROM python:3.10-slim

WORKDIR /app

# instala as dependências antes do código para aproveitar o cache da imagem
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# copia o código-fonte que será executado no container
COPY src/ ./src/

# cria o ponto usado pelo volume compartilhado
RUN mkdir -p /app/shared_folder

# a opção -u mostra os logs imediatamente e -m executa o pacote corretamente
CMD ["python", "-u", "-m", "src.main"]
