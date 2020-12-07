FROM node:14
WORKDIR /usr/src/app

RUN mkdir runtime

COPY package.json .
COPY runtime/index.js ./runtime
RUN npm install

EXPOSE 8000
CMD [ "npm", "start" ]
