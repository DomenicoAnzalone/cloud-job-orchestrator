const express = require("express");
const path = require("path");

const app = express();

app.get('/config.js', (req, res) => {
  const config = {
    API_BASE_URL: process.env.FRONTEND_API_BASE_URL,
    AZURE_AD_CLIENT_ID: process.env.AZURE_AD_CLIENT_ID,
    AZURE_AD_TENANT_ID: process.env.AZURE_AD_TENANT_ID,
    AZURE_AD_API_AUDIENCE: process.env.AZURE_AD_API_AUDIENCE,
    AZURE_AD_API_SCOPE: process.env.AZURE_AD_API_SCOPE || null
  };

  res.type('application/javascript');
  res.send(`window.APP_CONFIG = ${JSON.stringify(config)};`);
});

// serve file statici da wwwroot
app.use(express.static(__dirname));

// fallback su index.html
app.get("*", (req, res) => {
  res.sendFile(path.join(__dirname, "index.html"));
});

const port = process.env.PORT || 3000;
app.listen(port, () => {
  console.log("Server running on port", port);
});