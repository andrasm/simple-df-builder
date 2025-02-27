#!/bin/bash
set -e
pushd Bendo.SimpleDataFrameBuilder
rm bin/Release/Bendo.SimpleDataFrameBuilder.*.nupkg
dotnet pack --configuration Release
dotnet nuget push bin/Release/Bendo.SimpleDataFrameBuilder.*.nupkg --source https://api.nuget.org/v3/index.json --api-key $NUGET_API_KEY
popd