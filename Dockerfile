FROM microsoft/dotnet:2.1-sdk AS build
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

FROM microsoft/dotnet:2.1-runtime AS runtime
WORKDIR /app
COPY --from=build /app/albiondata-deduper-dotNet/out ./
ENTRYPOINT ["dotnet", "albiondata-deduper-dotNet.dll"]
