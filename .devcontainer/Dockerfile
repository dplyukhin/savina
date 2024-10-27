FROM sbtscala/scala-sbt:eclipse-temurin-jammy-17.0.10_7_1.10.2_2.13.15

# Install git and curl
RUN apt-get update
RUN apt-get install -y git python3

# Build Pekko/UIGC
RUN git clone --depth 1 https://github.com/dplyukhin/uigc-pekko.git /uigc-pekko
WORKDIR /uigc-pekko
RUN sbt publishLocal