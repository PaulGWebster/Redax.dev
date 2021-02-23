#See https://aka.ms/containerfastmode to understand how Visual Studio uses this Dockerfile to build your images for faster debugging.

FROM mcr.microsoft.com/dotnet/core/runtime:3.1-buster-slim AS base
WORKDIR /app

FROM mcr.microsoft.com/dotnet/core/sdk:3.1-buster AS build
WORKDIR /src
COPY ["RedisLoader/GDAX Stream reader.csproj", "RedisLoader/"]
COPY ["RedisLoaderLib/GDAX WS Client Lib.csproj", "RedisLoaderLib/"]
RUN dotnet restore "RedisLoader/GDAX Stream reader.csproj"
COPY . .
WORKDIR "/src/RedisLoader"
RUN dotnet build "GDAX Stream reader.csproj" -c Release -o /app/build

FROM build AS publish
RUN dotnet publish "GDAX Stream reader.csproj" -c Release -o /app/publish

FROM base AS final
WORKDIR /app
COPY --from=publish /app/publish .
ENTRYPOINT ["dotnet", "GDAX Stream reader.dll"]