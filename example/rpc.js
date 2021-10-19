module.exports = require("yajrpc/qup")({
  url: process.env.RPC || "http://localhost:8332",
  // auth: require('fs').readFileSync(process.env.RPCCOOKIE),
  user: process.env.RPCUSER,
  pass: process.env.RPCPASSWORD,
  batch: process.env.RPCBATCHSIZE || 500,
  concurrent: process.env.RPCCONCURRENT || 16
});
