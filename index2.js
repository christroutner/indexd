/*
  Primary library file.
  This will replace index.js
*/

// Public npm libraries.
const { EventEmitter } = require("events");
let parallel = require("run-parallel");

// Local libraries.
let dbwrapper = require("./dbwrapper");
let rpcUtil = require("./rpc");

// Indicies
let FeeIndex = require("./indexes/fee");
let MtpIndex = require("./indexes/mediantime");
let ScriptIndex = require("./indexes/script");
let TxIndex = require("./indexes/tx");
let TxinIndex = require("./indexes/txin");
let TxoIndex = require("./indexes/txo");

class Indexd {
  constructor(db, rpc) {
    this.db = dbwrapper(db);
    this.rpc = rpc;
    this.emitter = new EventEmitter(); // TODO: bind to this
    this.emitter.setMaxListeners(Infinity);
    this.indexes = {
      fee: new FeeIndex(),
      mtp: new MtpIndex(),
      script: new ScriptIndex(),
      tx: new TxIndex(),
      txin: new TxinIndex(),
      txo: new TxoIndex()
    };
  }

  // Compile an array of indexing functions to execute, then execute them in
  // parallel.
  tips(callback) {
    console.log("Entering tips");
    let tasks = {};

    console.log("this.indexes: ", this.indexes);

    for (let indexName in this.indexes) {
      let index = this.indexes[indexName];
      tasks[indexName] = next => index.tip(this.db, next);
    }
    console.log("tasks: ", tasks);

    parallel(tasks, callback);
  }

  // This is the main indexing function.
  // recurses until `nextBlockId` is falsy
  connectFrom(prevBlockId, blockId, callback) {
    this.tips((err, tips) => {
      if (err) return callback(err);

      let todo = {};
      for (let indexName in tips) {
        let tip = tips[indexName];
        if (tip && tip.blockId !== prevBlockId) continue;
        if (indexName === "fee") {
          if (!tips.txo) continue;
          if (tip && tips.fee.height > tips.txo.height) continue;
        }

        todo[indexName] = true;
      }

      let todoList = Object.keys(todo);
      if (todoList.length === 0)
        return callback(new RangeError("Misconfiguration"));

      console.log(`Downloading ${blockId} (for ${todoList})`);

      rpcUtil.block(this.rpc, blockId, (err, block) => {
        if (err) return callback(err);

        let atomic = this.db.atomic();
        let events; // TODO
        let { height } = block;
        console.log(`Connecting ${blockId} @ ${height}`);

        // connect block to relevant chain tips
        for (let indexName in todo) {
          let index = this.indexes[indexName];
          if (!index.connect) continue;

          index.connect(atomic, block, events);
        }

        atomic.write(err => {
          if (err) return callback(err);
          console.log(`Connected ${blockId} @ ${height}`);

          let self = this;
          function loop(err) {
            if (err) return callback(err);

            // recurse until nextBlockId is falsy
            if (!block.nextBlockId) return callback(null, true);
            self.connectFrom(blockId, block.nextBlockId, callback);
          }

          if (!todo.fee) return loop();

          console.log(`Connecting ${blockId} (2nd Order)`);
          let atomic2 = this.db.atomic();
          this.indexes.fee.connect2ndOrder(
            this.db,
            this.indexes.txo,
            atomic2,
            block,
            err => {
              if (err) return loop(err);

              console.log(`Connected ${blockId} (2nd Order)`);
              atomic2.write(loop);
            }
          );
        });
      });
    });
  }

  disconnect(blockId, callback) {
    console.log(`Disconnecting ${blockId}`);

    function fin(err) {
      if (err) return callback(err);
      console.log(`Disconnected ${blockId}`);
      callback();
    }

    this.tips((err, tips) => {
      if (err) return fin(err);

      // TODO: fetch lazily
      rpcUtil.block(this.rpc, blockId, (err, block) => {
        if (err) return fin(err);

        let atomic = this.db.atomic();

        // disconnect block from relevant chain tips
        for (let indexName in this.indexes) {
          let index = this.indexes[indexName];
          let tip = tips[indexName];
          if (!tip) continue;
          if (tip.blockId !== block.blockId) continue;

          index.disconnect(atomic, block);
        }

        atomic.write(fin);
      });
    });
  }

  // empties the mempool
  clear() {
    for (let indexName in this.indexes) {
      this.indexes[indexName].constructor();
    }
  }

  lowestTip(callback) {
    this.tips((err, tips) => {
      if (err) return callback(err);

      let lowest;
      for (let key in tips) {
        let tip = tips[key];
        if (!tip) return callback();
        if (!lowest) lowest = tip;
        if (lowest.height < tip.height) continue;
        lowest = tip;
      }

      callback(null, lowest);
    });
  }

  // Returns a Promise that resolves into an object.
  // The object has two properties: blockId and height
  // The object represents the syncing status of the indexer.
  lowestTip2() {
    return new Promise((resolves, rejects) => {
      this.tips((err, tips) => {
        if (err) return rejects(err);

        let lowest;
        for (let key in tips) {
          let tip = tips[key];

          if (!tip) return resolves();

          if (!lowest) lowest = tip;
          if (lowest.height < tip.height) continue;
          lowest = tip;
        }

        return resolves(lowest);
      });
    });
  }

  __resync(done) {
    console.log("resynchronizing");

    parallel(
      {
        bitcoind: f => rpcUtil.tip(this.rpc, f),
        indexd: f => this.lowestTip(f)
      },
      (err, r) => {
        if (err) return done(err);

        console.log("r: ", r);

        // Step 0, genesis?
        if (!r.indexd) {
          console.log("genesis");
          return rpcUtil.blockIdAtHeight(this.rpc, 0, (err, genesisId) => {
            if (err) return done(err);

            this.connectFrom(null, genesisId, done);
          });
        }

        // Step 1, equal?
        console.log("...", r);
        if (r.bitcoind.blockId === r.indexd.blockId) return done();

        // Step 2, is indexd behind? [aka, does bitcoind have the indexd tip]
        rpcUtil.headerJSON(this.rpc, r.indexd.blockId, (err, common) => {
          //        if (err && /not found/.test(err.message)) return fin(err) // uh, burn it to the ground
          if (err) return done(err);

          // forked?
          if (common.confirmations === -1) {
            console.log("forked");
            return this.disconnect(r.indexd.blockId, err => {
              if (err) return done(err);

              this.__resync(done);
            });
          }

          // yes, indexd is behind
          console.log("bitcoind is ahead");
          this.connectFrom(common.blockId, common.nextBlockId, done);
        });
      }
    );
  }

  async __resync2(done) {
    try {
      console.log("resynchronizing2");

      const r = {};

      r.bitcoind = await rpcUtil.tip2(this.rpc);
      console.log("r.bitcoind: ", r.bitcoind);

      r.indexd = await this.lowestTip2();
      console.log("r.indexd: ", r.indexd);

      // Analyze different startup states and react accordingly.

      // Syncing from Genesis
      if (!r.indexd) {
        console.log("genesis");
        return rpcUtil.blockIdAtHeight(this.rpc, 0, (err, genesisId) => {
          if (err) return done(err);

          this.connectFrom(null, genesisId, done);
        });
      }

      console.log("Indexd vs bitcoind status: ", r);

      // Indexer and full node are at the same height. This indicates the indexer
      // is fully synced. The syncing process can exit.
      if (r.bitcoind.blockId === r.indexd.blockId) return done();

      // Indexer is behind, and needs to synchronize to the blockchain.

      const header = await rpcUtil.headerJSON2(this.rpc, r.indexd.blockId);
      console.log("header: ", header);

      // Corner-case: The blockchain has forked, indicated by the 'confirmations'
      // property has a value of -1.
      if (header.confirmations === -1) {
        console.log("Corner Case: blockchain forked");
        return this.disconnect(r.indexd.blockId, err => {
          if (err) return done(err);

          this.__resync2(done);
        });
      }

      // Normal workflow: indexd is behind, start syncing.
      console.log("bitcoind is ahead. Synchronizing indexd");
      this.connectFrom(header.blockId, header.nextBlockId, done);
    } catch (err) {
      console.error("Error in __resync2()");
      throw err;
    }
  }

  tryResync(callback) {
    if (callback) {
      this.emitter.once("resync", callback);
    }

    if (this.syncing) return;
    this.syncing = true;

    let self = this;
    function fin(err) {
      self.syncing = false;
      self.emitter.emit("resync", err);
    }

    // this.__resync((err, updated) => {
    this.__resync2((err, updated) => {
      if (err) return fin(err);
      if (updated) return this.tryResyncMempool(fin);
      fin();
    });
  }

  tryResyncMempool(callback) {
    rpcUtil.mempool(this.rpc, (err, txIds) => {
      if (err) return callback(err);

      this.clear();
      parallel(
        txIds.map(txId => next => this.notify(txId, next)),
        callback
      );
    });
  }

  notify(txId, callback) {
    rpcUtil.transaction(this.rpc, txId, (err, tx) => {
      if (err) return callback(err);

      for (let indexName in this.indexes) {
        let index = this.indexes[indexName];

        if (!index.mempool) continue;
        index.mempool(tx);
      }

      callback();
    });
  }

  // QUERIES
  blockIdByTransactionId(txId, callback) {
    this.indexes.tx.heightBy(this.db, txId, (err, height) => {
      if (err) return callback(err);
      if (height === -1) return callback();

      rpcUtil.blockIdAtHeight(this.rpc, height, callback);
    });
  }

  latestFeesForNBlocks(nBlocks, callback) {
    this.indexes.fee.latestFeesFor(this.db, nBlocks, callback);
  }

  // returns a txo { txId, vout, value, script }, by key { txId, vout }
  txoByTxo(txo, callback) {
    this.indexes.txo.txoBy(this.db, txo, callback);
  }

  // returns the height at scId was first-seen (-1 if unconfirmed)
  firstSeenScriptId(scId, callback) {
    this.indexes.script.firstSeenScriptId(this.db, scId, callback);
  }

  // returns a list of txIds with inputs/outputs from/to a { scId, heightRange, ?mempool }
  transactionIdsByScriptRange(scRange, dbLimit, callback) {
    this.txosByScriptRange(scRange, dbLimit, (err, txos) => {
      if (err) return callback(err);

      let txIdSet = {};
      let tasks = txos.map(txo => {
        txIdSet[txo.txId] = true;
        return next => this.indexes.txin.txinBy(this.db, txo, next);
      });

      parallel(tasks, (err, txins) => {
        if (err) return callback(err);

        txins.forEach(txin => {
          if (!txin) return;
          txIdSet[txin.txId] = true;
        });

        callback(null, Object.keys(txIdSet));
      });
    });
  }

  // returns a list of txos { txId, vout, height, value } by { scId, heightRange, ?mempool }
  txosByScriptRange(scRange, dbLimit, callback) {
    this.indexes.script.txosBy(this.db, scRange, dbLimit, callback);
  }

  // returns a list of (unspent) txos { txId, vout, height, value }, by { scId, heightRange, ?mempool }
  // XXX: despite txo queries being bound by heightRange, the UTXO status is up-to-date
  utxosByScriptRange(scRange, dbLimit, callback) {
    this.txosByScriptRange(scRange, dbLimit, (err, txos) => {
      if (err) return callback(err);

      let taskMap = {};
      let unspentMap = {};

      txos.forEach(txo => {
        let txoId = txoToString(txo);
        unspentMap[txoId] = txo;
        taskMap[txoId] = next => this.indexes.txin.txinBy(this.db, txo, next);
      });

      parallel(taskMap, (err, txinMap) => {
        if (err) return callback(err);

        let unspents = [];
        for (let txoId in txinMap) {
          let txin = txinMap[txoId];

          // has a txin, therefore spent
          if (txin) continue;

          unspents.push(unspentMap[txoId]);
        }

        callback(null, unspents);
      });
    });
  }
}

module.exports = Indexd;
