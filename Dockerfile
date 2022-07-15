FROM mcr.microsoft.com/dotnet/core/sdk:3.1 AS build
WORKDIR /app

# copy csproj and restore as distinct layers
COPY albiondata-deduper-dotNet/*.csproj ./albiondata-deduper-dotNet/
WORKDIR /app/albiondata-deduper-dotNet
RUN dotnet restore

# copy and publish app and libraries
WORKDIR /app/
COPY albiondata-deduper-dotNet/. ./albiondata-deduper-dotNet/
WORKDIR /app/albiondata-deduper-dotNet
RUN dotnet publish -c Release -o out

FROM mcr.microsoft.com/dotnet/core/runtime:3.1 AS runtime
WORKDIR /app
COPY --from=build /app/albiondata-deduper-dotNet/out ./
ENTRYPOINT ["dotnet", "albiondata-deduper-dotNet.dll"]
