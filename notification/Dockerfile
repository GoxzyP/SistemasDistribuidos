# Lightweight Node LTS image
FROM node:22-alpine

WORKDIR /app

# Install dependencies first to take advantage of Docker layer caching
COPY package*.json ./
RUN npm ci --omit=dev

COPY notification.js ./

EXPOSE 8003
CMD ["node", "notification.js"]
