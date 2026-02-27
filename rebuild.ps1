git status
git add -A
git commit -m "v0.1.4: mask sensitive information"

# Vanilla release tag (this is what stable will point to)
git tag v0.1.4 -m "v0.1.4"

git push origin main
git push origin v0.1.4

$SHA = (git rev-parse --short HEAD).Trim()

# Build once, tag many (v0.1.4 + stable + sha; optionally latest)
docker build `
  -t ghcr.io/satoshiware/azcoin-stratum-gateway:v0.1.4 `
  -t ghcr.io/satoshiware/azcoin-stratum-gateway:stable `
  -t ghcr.io/satoshiware/azcoin-stratum-gateway:sha-$SHA `
  -t ghcr.io/satoshiware/azcoin-stratum-gateway:latest `
  .
  
docker push ghcr.io/satoshiware/azcoin-stratum-gateway:v0.1.4
docker push ghcr.io/satoshiware/azcoin-stratum-gateway:stable
docker push ghcr.io/satoshiware/azcoin-stratum-gateway:sha-$SHA
docker push ghcr.io/satoshiware/azcoin-stratum-gateway:latest