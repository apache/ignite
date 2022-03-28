# Build runtime image
FROM mcr.microsoft.com/dotnet/runtime:3.1-alpine

RUN apk --no-cache update && \
    apk --no-cache upgrade

RUN apk --no-cache add openjdk11 --repository=http://dl-cdn.alpinelinux.org/alpine/edge/community

# Copy binaries
WORKDIR /app
COPY netcoreapp3.1 .
COPY libs ./libs

# Run
ENV LD_LIBRARY_PATH /usr/lib/jvm/default-jvm/jre/lib/server
ENV COMPlus_EnableAlternateStackCheck 1

ENTRYPOINT ["dotnet", "Apache.Ignite.Executable.dll"]

