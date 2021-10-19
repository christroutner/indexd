/*
  Attempts to connect to a BCHN node and sync from genesis.
*/

// Adjust these constants, or move to a bash file.
process.env.RPC = "http://192.168.43.202:8332";
process.env.RPCUSER = "bitcoin";
process.env.RPCPASSWORD = "password";
process.env.INDEXDB = "testIndexd";
process.env.KEYDB = "KEYS";
process.env.DEBUG = "*";

const rpc = require("./rpc");
let leveldown = require("leveldown");
const db = leveldown(process.env.INDEXDB);

const Indexd = require("../index");
const indexd = new Indexd(db, rpc);

function errorSink(err) {
  if (err) console.error(err);
}

// Start synchronizing.
function startSync() {
  try {
    console.log(`Opening leveldb @ ${process.env.INDEXDB}`);
    db.open(
      {
        writeBufferSize: 1 * 1024 * 1024 * 1024 // 1 GiB
      },
      err => {
        if (err) throw err;
        console.log(`Opened leveldb @ ${process.env.INDEXDB}`);

        indexd.tryResync(errorSink);
      }
    );
  } catch (err) {
    console.error(err);
  }
}
startSync();
