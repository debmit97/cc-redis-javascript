const { toBulkString, toSimpleError } = require("./parseCommands");

let stream = null;
let timer = null;

class RedisStream {
  streamData = {};
  lastEntryKey = "0-0";

  validateXAdd(key, conn) {
    if (key.split("-")[1] === "*") {
      return true;
    }
    const keyMilliSec = parseInt(key.split("-")[0]);
    const keySeq = parseInt(key.split("-")[1]);
    const lastMilliSec = parseInt(this.lastEntryKey.split("-")[0]);
    const lastSeq = parseInt(this.lastEntryKey.split("-")[1]);
    if (keyMilliSec === 0 && keySeq === 0) {
      conn.write(
        toSimpleError("ERR The ID specified in XADD must be greater than 0-0")
      );
      return false;
    } else if (
      keyMilliSec < lastMilliSec ||
      (keyMilliSec === lastMilliSec && keySeq <= lastSeq)
    ) {
      conn.write(
        toSimpleError(
          "ERR The ID specified in XADD is equal or smaller than the target stream top item"
        )
      );
      return false;
    }
    return true;
  }

  autoGenEntryId(key, streamName) {
    function idSort(a, b) {
      return parseInt(a.split("-")[1]) > parseInt(b.split("-")[1]);
    }

    if (key === "*") {
      return `${Date.now()}-0`;
    }

    if (key.split("-")[1] === "*") {
      const keyMilliSec = parseInt(key.split("-")[0]);
      const keys = Object.keys(this.streamData[streamName])
        .filter((id) => parseInt(id.split("-")[0]) === keyMilliSec)
        .sort(idSort);
      if (keys.length) {
        return `${keys[keys.length - 1].split("-")[0]}-${
          parseInt(keys[keys.length - 1].split("-")[1]) + 1
        }`;
      } else {
        return `${key.split("-")[0]}-${
          parseInt(key.split("-")[0]) === 0 ? 1 : 0
        }`;
      }
    }

    return key;
  }

  handleXADD(xAddArgs, conn) {
    if (this.validateXAdd(xAddArgs[1], conn)) {
      if (!this.streamData[xAddArgs[0]]) {
        this.streamData[xAddArgs[0]] = {};
      }
      const id = this.autoGenEntryId(xAddArgs[1], xAddArgs[0]);
      this.streamData[xAddArgs[0]][id] = { [xAddArgs[2]]: xAddArgs[3] };
      this.lastEntryKey = id;
      conn.write(toBulkString(id));
    }
  }

  getMaxStreamKey(streamName) {

    function keySort(a, b) {
      const aMilli = parseInt(a.split('-')[0])
      const aSeq = parseInt(a.split('-')[1])

      const bMilli = parseInt(b.split('-')[0])
      const bSeq = parseInt(b.split('-')[1])

      if(aMilli > bMilli) {
        return true
      }

      if(aMilli < bMilli) {
        return false
      }

      if(aSeq > bSeq) {
        return true
      }

      return false
    }
    return Object.keys(this.streamData[streamName]).sort(keySort)[0]
  }

  handleXread(xReadArgs, conn) {
    if (xReadArgs[0].toUpperCase() === "BLOCK") {
      const blockPeriod = xReadArgs[1];

      if (blockPeriod !== "0") {
        const nowString = this.getXreadResp(xReadArgs.slice(2));
        setTimeout(
          (nowString, conn) => {
            const newString = this.getXreadResp(xReadArgs.slice(2));
            if (newString === nowString) {
              conn.write("$-1\r\n");
            } else {
              conn.write(newString);
            }
          },
          blockPeriod,
          nowString,
          conn
        );
      } else {
        if (xReadArgs.includes("$")) {
          console.log(xReadArgs)
          const streams = [];
          const args = [];

          const xReadArgsNew = ['streams']

          for (
            let i = xReadArgs.findIndex((e) => e === "streams") + 1;
            i < xReadArgs.length;
            i++
          ) {
            if (xReadArgs[i].includes("-")) {
              args.push(xReadArgs[i]);
              xReadArgsNew.push(xReadArgs[i])
            } else if (xReadArgs[i] === '$') {
              args.push(this.getMaxStreamKey(streams[args.length]))
              xReadArgsNew.push(args[args.length-1])
            } else {
              streams.push(xReadArgs[i]);
              xReadArgsNew.push(xReadArgs[i])
            }
          }

          const nowString = this.getXreadResp(xReadArgsNew);
          timer = setInterval(
            (nowString, conn) => {
              const newString = this.getXreadResp(xReadArgsNew);
              if (newString !== nowString) {
                conn.write(newString);
                clearInterval(timer);
              }
            },
            1000,
            nowString,
            conn
          );
        } else {
          const nowString = this.getXreadResp(xReadArgs.slice(2));
          timer = setInterval(
            (nowString, conn) => {
              const newString = this.getXreadResp(xReadArgs.slice(2));
              if (newString !== nowString) {
                conn.write(newString);
                clearInterval(timer);
              }
            },
            1000,
            nowString,
            conn
          );
        }
      }
    } else {
      conn.write(this.getXreadResp(xReadArgs));
    }
  }

  getXreadResp(xReadArgs) {
    function filterRange(startId, id) {
      if (
        parseInt(id.split("-")[0]) > parseInt(startId.split("-")[0]) ||
        (parseInt(id.split("-")[0]) === parseInt(startId.split("-")[0]) &&
          parseInt(id.split("-")[1]) > parseInt(startId.split("-")[1]))
      ) {
        return true;
      }
      return false;
    }

    const streams = [];
    const args = [];

    for (
      let i = xReadArgs.findIndex((e) => e === "streams") + 1;
      i < xReadArgs.length;
      i++
    ) {
      if (xReadArgs[i].includes("-")) {
        args.push(xReadArgs[i]);
      } else {
        streams.push(xReadArgs[i]);
      }
    }

    let resp = "";
    for (let i = 0; i < streams.length; i++) {
      const startId = args[i];
      const keys = Object.keys(this.streamData[streams[i]]).filter((id) =>
        filterRange(startId, id)
      );
      resp = resp + this.formatXreadInnerKeys(keys, streams[i]);
    }

    return `*${streams.length}\r\n${resp}`;
  }

  handleXRange(xRangeArgs, conn) {
    function filterRange(startId, endId, id) {
      if (startId === "-") {
        if (
          parseInt(id.split("-")[0]) <= parseInt(endId.split("-")[0]) &&
          parseInt(id.split("-")[1]) <= parseInt(endId.split("-")[1])
        ) {
          return true;
        }
        return false;
      }

      if (endId === "+") {
        if (
          parseInt(id.split("-")[0]) >= parseInt(startId.split("-")[0]) &&
          parseInt(id.split("-")[1]) >= parseInt(startId.split("-")[1])
        ) {
          return true;
        }
        return false;
      }

      if (
        parseInt(id.split("-")[0]) >= parseInt(startId.split("-")[0]) &&
        parseInt(id.split("-")[1]) >= parseInt(startId.split("-")[1]) &&
        parseInt(id.split("-")[0]) <= parseInt(endId.split("-")[0]) &&
        parseInt(id.split("-")[1]) <= parseInt(endId.split("-")[1])
      ) {
        return true;
      }
      return false;
    }

    const startId = xRangeArgs[1];
    const endId = xRangeArgs[2];
    const keys = Object.keys(this.streamData[xRangeArgs[0]]).filter((id) =>
      filterRange(startId, endId, id)
    );
    conn.write(this.formatInnerKeys(keys, xRangeArgs[0]));
  }

  formatXreadInnerKeys(keys, stream) {
    let resp = "";
    for (let key of keys) {
      resp = resp + `${objectToArray({ [key]: this.streamData[stream][key] })}`;
    }
    return `*2\r\n$${stream.length}\r\n${stream}\r\n*${keys.length}\r\n${resp}`;
  }

  formatInnerKeys(keys, stream) {
    let resp = "";
    for (let key of keys) {
      resp = resp + `${objectToArray({ [key]: this.streamData[stream][key] })}`;
    }
    return `*${keys.length}\r\n${resp}`;
  }
}

function objectToArray(obj) {
  let resp = "";
  for (let key of Object.keys(obj)) {
    resp = resp + `$${key.length}\r\n${key}\r\n`;
    if (typeof obj[key] === "object") {
      resp = resp + objectToArray(obj[key]);
    } else {
      resp = resp + `$${obj[key].length}\r\n${obj[key]}\r\n`;
    }
  }
  return `*${Object.keys(obj).length * 2}\r\n${resp}`;
}

function getStream() {
  if (!stream) {
    stream = new RedisStream();
  }
  return stream;
}

module.exports = { getStream, RedisStream };
