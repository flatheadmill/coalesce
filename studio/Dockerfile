FROM node:22-alpine AS build

WORKDIR /build/studio
COPY studio/package.json studio/package-lock.json ./
RUN npm ci
COPY studio/ ./
RUN npm run build

FROM caddy:2-alpine

COPY studio/Caddyfile /etc/caddy/Caddyfile
COPY --from=build /build/studio/dist/one /srv/one
COPY --from=build /build/studio/dist/two /srv/two
COPY --from=build /build/studio/dist/three /srv/three

ENV COALESCE_UPSTREAM=http://coalesce.coalesce.svc.cluster.local
ENV COALESCE_ONE_HOST=one.localhost
ENV COALESCE_TWO_HOST=two.localhost
ENV COALESCE_THREE_HOST=three.localhost

EXPOSE 80
