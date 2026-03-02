git status
git add -A
git commit -m "v0.1.4-final: pivoting to use SRI"

# Vanilla release tag (this is what stable will point to)
git tag v0.1.4-final -m "v0.1.4-final"

git push origin main
git push origin v0.1.4-final 

$SHA = (git rev-parse --short HEAD).Trim()

# Build once, tag many (v0.1.4 + stable + sha; optionally latest)
docker build `
  -t ghcr.io/satoshiware/azcoin-stratum-gateway:sha-$SHA `
  -t ghcr.io/satoshiware/azcoin-stratum-gateway:latest `
  .

docker push ghcr.io/satoshiware/azcoin-stratum-gateway:sha-$SHA
docker push ghcr.io/satoshiware/azcoin-stratum-gateway:latest