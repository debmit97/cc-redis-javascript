const { toBulkString, toSimpleError } = require('./parseCommands')

let stream = null

class RedisStream {
    streamName = ''
    streamData = {}
    lastEntryKey = ''

    validateXAdd(key, conn) {
        const keyMilliSec = parseInt(key.split('-')[0])
        const keySeq = parseInt(key.split('-')[1])
        const lastMilliSec = parseInt(key.split('-')[0])
        const lastSeq = parseInt(key.split('-')[1])
        if(keyMilliSec === 0 && keySeq === 0) {
            conn.write(toSimpleError('ERR The ID specified in XADD must be greater than 0-0'))
            return false
        } else if (keyMilliSec < lastMilliSec || keyMilliSec === lastMilliSec && keySeq < lastSeq) {
            conn.write(toSimpleError('ERR The ID specified in XADD is equal or smaller than the target stream top item'))
            return false
        }
        return true
    }

    handleXADD(xAddArgs, conn) {
        if(this.validateXAdd(xAddArgs[1], conn)) {

            this.streamName = xAddArgs[0]
            this.streamData[xAddArgs[1]] = { [xAddArgs[2]]: xAddArgs[3] }
            this.lastEntryKey = xAddArgs[1]
            conn.write(toBulkString(xAddArgs[1]))
        }
    }
}

function getStream() {
    if(!stream) {
        stream = new RedisStream()
    }
    return stream
}

module.exports = { getStream, RedisStream }