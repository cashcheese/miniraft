FROM node:20-alpine
WORKDIR /app
COPY package.json .
RUN npm install
CMD ["npx", "nodemon", "--legacy-watch", "server.js"]
