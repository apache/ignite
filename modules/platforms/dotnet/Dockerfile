# Build runtime image
FROM mcr.microsoft.com/dotnet/runtime:3.1-alpine

RUN apk --no-cache update && \
    apk --no-cache upgrade

RUN apk --no-cache add openjdk8 --repository=http://dl-cdn.alpinelinux.org/alpine/edge/community

# Copy binaries
WORKDIR /app
COPY netcoreapp3.1 .
COPY libs .

# Run
ENTRYPOINT ["dotnet", "Apache.Ignite.Executable.dll"]

