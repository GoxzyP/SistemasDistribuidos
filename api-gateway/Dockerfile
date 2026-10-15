# Imagem leve com Node LTS
FROM node:22-alpine

WORKDIR /app

# Dependências primeiro, para aproveitar a cache de camadas do Docker
COPY package*.json ./
RUN npm ci --omit=dev

COPY api-gateway.js ./

EXPOSE 8000
CMD ["node", "api-gateway.js"]
