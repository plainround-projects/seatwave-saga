# The payment provider's stub (Module 7's stand-in). Two stages: the JDK compiles the one source
# file, and only its classes go into the runtime image, which runs as a user without privileges.
# Base images pinned by tag and digest, checked on Docker Hub on 12 November 2026 (arm64 present).
FROM eclipse-temurin:25.0.5_8-jdk-noble@sha256:80f67fb3d6f76e2d817d98a4cc2a9cf05f054d4e6adc5e2144bde671d722e63f AS build
WORKDIR /build
COPY src/main/java/io/plainround/seatwave/saga/stub/PaymentProviderStub.java .
RUN javac -Xlint:all -Werror --release 25 -d /out PaymentProviderStub.java

FROM eclipse-temurin:25.0.5_8-jre-noble@sha256:e3b6f7887a50f20d8f07abf279d8a81a5953fb398a9555bd9edd56b9bd15968c
COPY --from=build /out /opt/stub
USER 10001:10001
EXPOSE 8080
ENTRYPOINT ["java", "-cp", "/opt/stub", "io.plainround.seatwave.saga.stub.PaymentProviderStub"]
