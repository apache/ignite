set ME_CONFIG_MONGODB_ENABLE_ADMIN=true
set ME_CONFIG_MONGODB_URL=mongodb://localhost:27018/admin?ssl=false
set ME_CONFIG_MONGODB_AUTH_DATABASE=admin
set ME_CONFIG_MONGODB_SSL=false
set ME_CONFIG_SITE_GRIDFS_ENABLED=true
set ME_CONFIG_BASICAUTH=true

yarn start

REM node --inspect app.js