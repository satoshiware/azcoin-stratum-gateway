git status
git add -A
git commit -m "v0.1.4: revision needs to mask sensitive information"
git tag v0.1.4-rev1 -m "v0.1.4"
git push origin main
git push origin v0.1.4-rev1

docker build -t ghcr.io/satoshiware/azcoin-stratum-gateway:v0.1.4 .

docker push ghcr.io/satoshiware/azcoin-stratum-gateway:v0.1.4