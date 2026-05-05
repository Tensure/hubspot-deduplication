# syntax=docker/dockerfile:1.7

FROM node:22-bookworm-slim AS deps
WORKDIR /app

COPY package.json package-lock.json ./
RUN npm ci --omit=dev && npm cache clean --force

FROM gcr.io/distroless/nodejs22-debian12:nonroot

ENV NODE_ENV=production

COPY --from=deps --chown=65532:65532 /app/node_modules /home/nonroot/app/node_modules
COPY --chown=65532:65532 package.json /home/nonroot/app/package.json
COPY --chown=65532:65532 src /home/nonroot/app/src

WORKDIR /home/nonroot/app

USER 65532:65532

CMD ["src/hubspotMergeEngine.js"]
