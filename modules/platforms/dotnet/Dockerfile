# Build runtime image
FROM mcr.microsoft.com/dotnet/runtime:3.1-alpine

RUN apk --no-cache update && \
    apk --no-cache upgrade

RUN apk --no-cache add openjdk8 --repository=http://dl-cdn.alpinelinux.org/alpine/edge/community

# Copy binaries
WORKDIR /app
COPY netcoreapp3.1 .
COPY libs ./libs

# RUN chgrp -R 0 /app && \
#     chmod -R g=u /app
#
# USER 10000

# Run
ENV LD_LIBRARY_PATH /usr/lib/jvm/default-jvm/jre/lib/amd64/server
ENV IGNITE_HOME /app

ENTRYPOINT ["dotnet", "Apache.Ignite.Executable.dll"]

