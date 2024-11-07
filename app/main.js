const net = require("net");
const fs = require("fs");
const { RDBParser } = require("./parseRDB.js");
const { parsedCommands } = require('./parseCommands.js')

let store = new Map();
const replicaConnections = []
const env = {};

function handlePing(conn) {
  conn.write("+PONG\r\n")
}

function handleEcho(echoArg, conn) {
  conn.write(`$${echoArg[0].length}\r\n${echoArg[0]}\r\n`);
}

function stringToRespArray(commandString) {
  const tokens = commandString.split(' ')
  let resp = ''
  for(const token of tokens) {
    resp = resp+`$${token.length}\r\n${token}\r\n`
  }
  return `*${tokens.length}\r\n${resp}`
}

function handleSet(setArgs, conn) {
  const [key, value] = setArgs;
  store.set(key, { value });
  if (setArgs.length > 2) {
    if (setArgs[2].toUpperCase() === "PX") {
      store.set(key, { value, expiration: Date.now() + parseInt(setArgs[3]) });
    }
  }
  if(!env.replicaof) { // it is master instance

    conn.write("+OK\r\n");
    replicaConnections.forEach(replicaConnection => {
      replicaConnection.write(stringToRespArray(`SET ${setArgs.join(' ')}`))
    })
  }
}

function handleGet(getArg, conn) {
  const [key] = getArg;
  if (
    store.has(key) &&
    (!store.get(key).expiration ||
      (store.get(key).expiration &&
        store.get(key).expiration > BigInt(Date.now())))
  ) {
    conn.write(`$${store.get(key).value.length}\r\n${store.get(key).value}\r\n`);
  } else {

    conn.write(`$-1\r\n`);
    
  }
}

function handleConfig(configArgs, conn) {
  const [command, arg] = configArgs;
  if (command.toUpperCase() === "GET") {
    if (arg === "dir") {
      conn.write(`*2\r\n$3\r\ndir\r\n$${process.argv[3].length}\r\n${process.argv[3]}\r\n`);
    } else if (arg === "dbfilename") {
      conn.write(`*2\r\n$10\r\ndbfilename\r\n$${process.argv[5].length}\r\n${process.argv[5]}\r\n`);
    }
  }
}

function handleKeys(conn) {
  let response = "";
  for (let key of store.keys()) {
    response += `$${key.length}\r\n${key}\r\n`;
  }
  conn.write(`*${store.size}\r\n${response}`);
}

function handleInfo(infoArgs, conn) {
  const [section] = infoArgs;
  switch (section.toUpperCase()) {
    case "REPLICATION":
      if (env.replicaof) {
        conn.write(`$10\r\nrole:slave\r\n`);
      } else {

        conn.write(`$89\r\nrole:master\r\nmaster_replid:8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb\r\nmaster_repl_offset:0\r\n`);
      }
  }
}

function handlePsync(psyncArgs, conn) {
  conn.write('+FULLRESYNC 8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb 0\r\n')
  const emptyRDBHex = '524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2'
  const fileBuffer = Buffer.from(emptyRDBHex, 'hex')
  const lenBuffer = Buffer.from(`$${fileBuffer.length}\r\n`)
  conn.write(Buffer.concat([lenBuffer, fileBuffer]))
  replicaConnections.push(conn)
}

function commandResponse(commandString, conn) {
  const commandArray = commandString.split(" ");
  switch (commandArray[0].toUpperCase()) {
    case "PING":
      handlePing(conn)
      break
    case "ECHO":
      handleEcho(commandArray.slice(1), conn)
      break
    case "SET":
      handleSet(commandArray.slice(1), conn);
      break
    case "GET":
      handleGet(commandArray.slice(1), conn);
      break
    case "CONFIG":
      handleConfig(commandArray.slice(1), conn);
      break
    case "KEYS":
      handleKeys(conn);
      break
    case "INFO":
      handleInfo(commandArray.slice(1), conn);
      break
    case "PSYNC":
      handlePsync(commandArray.slice(1), conn);
      break
    default:
      if(!env.replicaof) {

        conn.write("+OK\r\n");
      }
  }
}

function commandParser(commandString) {
  const commandSplit = commandString.split("\r\n");
  const numArgs = parseInt(commandSplit[0].substring(1));
  let len = 0;
  let string = "";
  for (let i = 0; i < numArgs; i++) {
    len = parseInt(commandSplit[i * 2 + 1].substring(1));
    string = string + ` ${commandSplit[i * 2 + 2].substring(0, len)}`;
  }
  return string.trim();
}

const server = net.createServer((connection) => {
  connection.on("data", (data) => {
    const command = commandParser(data.toString());
    commandResponse(command, connection)
  });
  connection.on("error", (e) => {
    console.log(e);
  });
});

function loadRDBFile() {
  let filePath = `${process.argv[3]}/${process.argv[5]}`;
  if (!fs.existsSync(filePath)) return;
  const fileBuffer = fs.readFileSync(filePath);
  let rdbParser = new RDBParser(fileBuffer);
  rdbParser.parse();
  store = rdbParser.dataStore;
}

loadRDBFile();
loadEnvs();

if (env.replicaof) {
  const conn = net.createConnection(
    { host: env.replicaof.split(" ")[0], port: env.replicaof.split(" ")[1] },
    () => {
      conn.write(`*1\r\n$4\r\nPING\r\n`);
      conn.on("data", (data) => {
        if (data.toString("utf-8") === "+PONG\r\n") {
          conn.write(
            `*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$4\r\n${getPort()}\r\n`
          );
          conn.write("*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n");
        } else if (data.toString("utf-8") === "+OK\r\n") {
          conn.write("*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n");
        } else {
          if(data.toString('utf-8').startsWith('*')) {
            for(const command of parsedCommands(data)) {
              console.log(command)
              commandResponse(command, conn)
            }
          }
        }
      });
    }
  );
  conn.on("error", (e) => {
    console.log(e);
  });
}

server.listen(getPort(), "127.0.0.1");

function getPort() {
  return env["port"] ? env["port"] : 6379;
}

function loadEnvs() {
  for (let i = 2; i < process.argv.length; i = i + 2) {
    env[process.argv[i].split("--")[1]] = process.argv[i + 1];
  }
  // console.log(env)
}
