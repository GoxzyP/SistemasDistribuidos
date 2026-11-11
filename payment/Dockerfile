# Lightweight Node LTS image
FROM node:22-alpine

WORKDIR /app

# Install dependencies first to take advantage of Docker layer caching
COPY package*.json ./
RUN npm ci --omit=dev

COPY payment.js ./

EXPOSE 8004
CMD ["node", "payment.js"]
