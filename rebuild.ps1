cd C:\AZCoin\01_PROJECTS\azcoin-stratum-gateway
git status
git add -A
git commit -m "feat(stratum): support configure/subscribe/authorize for Avalon"
git tag v0.1.3
git push origin main
git push origin v0.1.3
# Build the image (adjust Dockerfile path if needed)
docker build -t ghcr.io/satoshiware/azcoin-stratum-gateway:v0.1.3 .

# Push it to GHCR
docker push ghcr.io/satoshiware/azcoin-stratum-gateway:v0.1.3