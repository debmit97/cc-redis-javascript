const net = require("net");
const fs = require("fs")
const { RDBParser } = require("./parseRDB.js");

let store = new Map();
const env = {}

function handlePing() {
  return "+PONG\r\n";
}

function handleEcho(echoArg) {
  return `$${echoArg[0].length}\r\n${echoArg[0]}\r\n`;
}

function handleSet(setArgs) {
  const [key, value] = setArgs;
  store.set(key, {value});
  if (setArgs.length > 2) {
    if (setArgs[2].toUpperCase() === "PX") {
      store.set(key, {value, expiration: Date.now()+parseInt(setArgs[3])});
    } 
  }
  return "+OK\r\n";
}

function handleGet(getArg) {
  const [key] = getArg;
  if (store.has(key) && (!store.get(key).expiration || (store.get(key).expiration && store.get(key).expiration > BigInt(Date.now())))) {
    return `$${store.get(key).value.length}\r\n${store.get(key).value}\r\n`;
  }
  return `$-1\r\n`;
}

function handleConfig(configArgs) {
  const [command, arg] = configArgs;
  if (command.toUpperCase() === "GET") {
    if (arg === "dir") {
      return `*2\r\n$3\r\ndir\r\n$${process.argv[3].length}\r\n${process.argv[3]}\r\n`;
    } else if (arg === "dbfilename") {
      return `*2\r\n$10\r\ndbfilename\r\n$${process.argv[5].length}\r\n${process.argv[5]}\r\n`;
    }
  }
}

function handleKeys() {
  let response = "";
  for (let key of store.keys()) {
    response += `$${key.length}\r\n${key}\r\n`;
  }
  return `*${store.size}\r\n${response}`
}

function handleInfo(infoArgs) {
  const [section] = infoArgs
  switch(section.toUpperCase()) {
    case 'REPLICATION':
      if(env.replicaof) {
        return `$10\r\nrole:slave\r\n`
      }
      return `$89\r\nrole:master\r\nmaster_replid:8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb\r\nmaster_repl_offset:0\r\n`
  }
}

function commandResponse(commandString) {
  const commandArray = commandString.split(" ");
  switch (commandArray[0].toUpperCase()) {
    case "PING":
      return handlePing();
    case "ECHO":
      return handleEcho(commandArray.slice(1));
    case "SET":
      return handleSet(commandArray.slice(1));
    case "GET":
      return handleGet(commandArray.slice(1));
    case "CONFIG":
      return handleConfig(commandArray.slice(1));
    case "KEYS":
      return handleKeys();
    case "INFO":
      return handleInfo(commandArray.slice(1));
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
    connection.write(commandResponse(command));
  });
});

function loadRDBFile () {
  let filePath = `${process.argv[3]}/${process.argv[5]}`;
  if (!fs.existsSync(filePath)) return;
  const fileBuffer = fs.readFileSync(filePath);
  let rdbParser = new RDBParser(fileBuffer);
  rdbParser.parse();
  store = rdbParser.dataStore;
  console.log(store)
}

loadRDBFile()
loadEnvs()

if(env.replicaof) {
  const conn = net.createConnection({ host: env.replicaof.split(' ')[0], port: env.replicaof.split(' ')[1] })
  conn.write(`*1\r\n$4\r\nPING\r\n`)
}

server.listen(env['port'] ? env['port'] : 6379, "127.0.0.1");



function loadEnvs() {
  for(let i=2;i<process.argv.length;i = i+2) {
    env[process.argv[i].split('--')[1]] = process.argv[i+1]
  }
  // console.log(env)
}
