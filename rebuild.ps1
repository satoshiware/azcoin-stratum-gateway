Set-Location C:\AZCoin\01_PROJECTS\azcoin-stratum-gateway
git status
git add -A
git commit -m "v0.1.3: fixing gateway"
git tag v0.1.3 -m "v0.1.3"
git push origin main
git push origin v0.1.3

docker build -t ghcr.io/satoshiware/azcoin-stratum-gateway:v0.1.3 .

docker push ghcr.io/satoshiware/azcoin-stratum-gateway:v0.1.3