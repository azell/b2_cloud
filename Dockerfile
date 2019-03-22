FROM oracle/graalvm-ce:1.0.0-rc14 AS build
COPY build/libs/b2-cloud-1.0-dev-all.jar b2-cloud.jar
RUN native-image \
  --enable-https \
  --static \
  --verbose \
  -jar b2-cloud.jar

FROM scratch
COPY --from=build b2-cloud .
CMD ["/b2-cloud"]
