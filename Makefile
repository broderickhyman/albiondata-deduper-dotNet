all:
	dotnet run --project ./albiondata-deduper-dotNet

release:
	dotnet publish -c Release ./albiondata-deduper-dotNet
