# Imagem leve com Node LTS
FROM node:22-alpine

WORKDIR /app

# Dependências primeiro, para aproveitar a cache de camadas do Docker
COPY package*.json ./
RUN npm ci --omit=dev

COPY catalog.js ./
COPY data ./data

EXPOSE 8001
CMD ["node", "catalog.js"]
