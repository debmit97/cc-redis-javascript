const { toBulkString } = require('./parseCommands')

let stream = null

class RedisStream {
    streamName = ''
    streamData = {}

    handleXADD(xAddArgs, conn) {
        this.streamName = xAddArgs[0]
        this.streamData[xAddArgs[1]] = { [xAddArgs[2]]: xAddArgs[3] }
        conn.write(toBulkString(xAddArgs[1]))
    }
}

function getStream() {
    if(!stream) {
        stream = new RedisStream()
    }
    return stream
}

module.exports = { getStream, RedisStream }