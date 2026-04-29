FROM gcr.io/distroless/static-debian12:nonroot

ARG TARGETPLATFORM

WORKDIR /

COPY ${TARGETPLATFORM}/stevy /stevy
COPY public/ /public/

ENTRYPOINT ["/stevy"]

CMD ["serve"]