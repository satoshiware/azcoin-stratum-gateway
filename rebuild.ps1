git status
git add -A
git commit -m "v0.1.4: End of phase 2, release version 0.1.4"
git tag v0.1.4 -m "v0.1.4"
git push origin main
git push origin v0.1.4

docker build -t ghcr.io/satoshiware/azcoin-stratum-gateway:v0.1.4 .

docker push ghcr.io/satoshiware/azcoin-stratum-gateway:v0.1.4