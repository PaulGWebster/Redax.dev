#See https://aka.ms/containerfastmode to understand how Visual Studio uses this Dockerfile to build your images for faster debugging.

FROM mcr.microsoft.com/dotnet/core/runtime:3.1-buster-slim AS base
WORKDIR /app

FROM mcr.microsoft.com/dotnet/core/sdk:3.1-buster AS build
WORKDIR /src
COPY ["Redis loader/Redis Loader.csproj", "Redis loader/"]
COPY ["Redis Loader Lib/Redis Loader Lib.csproj", "Redis Loader Lib/"]
RUN dotnet restore "Redis loader/Redis Loader.csproj"
COPY . .
WORKDIR "/src/Redis loader"
RUN dotnet build "Redis Loader.csproj" -c Release -o /app/build

FROM build AS publish
RUN dotnet publish "Redis Loader.csproj" -c Release -o /app/publish

FROM base AS final
WORKDIR /app
COPY --from=publish /app/publish .
ENTRYPOINT ["dotnet", "Redis Loader.dll"]
