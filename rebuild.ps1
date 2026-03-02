git status
git add -A
git commit -m "v0.1.4-rev1: segwit rules"

# Vanilla release tag (this is what stable will point to)
git tag v0.1.4-rev1 -m "v0.1.4-rev1"

git push origin main
git push origin v0.1.4-rev1 

$SHA = (git rev-parse --short HEAD).Trim()

# Build once, tag many (v0.1.4 + stable + sha; optionally latest)
docker build `
  -t ghcr.io/satoshiware/azcoin-stratum-gateway:sha-$SHA `
  -t ghcr.io/satoshiware/azcoin-stratum-gateway:latest `
  .

docker push ghcr.io/satoshiware/azcoin-stratum-gateway:sha-$SHA
docker push ghcr.io/satoshiware/azcoin-stratum-gateway:latest