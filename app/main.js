const net = require("net");

const store = new Map()

function handlePing() {
    return '+PONG\r\n'
}

function handleEcho(echoArg) {
    return `$${echoArg[0].length}\r\n${echoArg[0]}\r\n`
}

function handleSet(setArgs) {
    const [key, value] = setArgs
    store.set(key, value)
    if(setArgs.length > 2) {
        if(setArgs[2].toUpperCase() === 'PX') {
            setTimeout(() => {
                store.delete(key)
            }, parseInt(setArgs[3]))
        }
    }
    return '+OK\r\n'
}

function handleGet(getArg) {
    const [key] = getArg
    if(store.has(key)) {
        return `$${store.get(key).length}\r\n${store.get(key)}\r\n`
    }
    return `$-1\r\n`
}

function commandResponse(commandString) {
    const commandArray = commandString.split(' ')
    switch (commandArray[0].toUpperCase()) {
        case 'PING':
            return handlePing()
        case 'ECHO':
            return handleEcho(commandArray.slice(1))
        case 'SET':
            return handleSet(commandArray.slice(1))
        case 'GET':
            return handleGet(commandArray.slice(1))
    }
}


function commandParser(commandString) {
    const commandSplit = commandString.split('\r\n')
    const numArgs = parseInt(commandSplit[0].substring(1))
    let len = 0
    let string = ''
    for (let i = 0; i < numArgs; i++) {
        len = parseInt(commandSplit[i * 2 + 1].substring(1))
        string = string + ` ${commandSplit[i * 2 + 2].substring(0, len)}`
    }
    return string.trim()
}

// Uncomment this block to pass the first stage
const server = net.createServer((connection) => {
    connection.on('data', (data) => {
        const command = commandParser(data.toString())
        connection.write(commandResponse(command))
    });
});

server.listen(6379, "127.0.0.1");
