#!/bin/sh
# Build CinephileCrossroads Synology package (.spk)
set -e

VERSION="2.0.0"
PKG_NAME="CinephileCrossroads"
BUILD_DIR="/tmp/spk_build"

rm -rf "$BUILD_DIR"
mkdir -p "$BUILD_DIR/package" "$BUILD_DIR/scripts" "$BUILD_DIR/conf"

# Package contents
cp app.py agent.py "$BUILD_DIR/package/"

# Package tarball
cd "$BUILD_DIR/package"
tar czf "$BUILD_DIR/package.tgz" .

# Scripts
cp synology/scripts/start-stop-status "$BUILD_DIR/scripts/"
chmod +x "$BUILD_DIR/scripts/start-stop-status"

# Info
cp synology/INFO "$BUILD_DIR/"

# Resource config
cat > "$BUILD_DIR/conf/resource" << 'EOF'
{
    "port-config": {
        "protocol-file": "synology/conf/protocol",
        "port": 8000
    }
}
EOF

# Build SPK
cd "$BUILD_DIR"
tar cf "../${PKG_NAME}-${VERSION}.spk" INFO package.tgz scripts/ conf/

echo "Built: ${PKG_NAME}-${VERSION}.spk"
rm -rf "$BUILD_DIR"
