const { toBulkString, toSimpleError } = require('./parseCommands')

let stream = null

class RedisStream {
    streamName = ''
    streamData = {}
    lastEntryKey = '0-0'

    validateXAdd(key, conn) {
        if(key.split('-')[1] === '*') {
            return true
        }
        const keyMilliSec = parseInt(key.split('-')[0])
        const keySeq = parseInt(key.split('-')[1])
        const lastMilliSec = parseInt(this.lastEntryKey.split('-')[0])
        const lastSeq = parseInt(this.lastEntryKey.split('-')[1])
        if(keyMilliSec === 0 && keySeq === 0) {
            conn.write(toSimpleError('ERR The ID specified in XADD must be greater than 0-0'))
            return false
        } else if (keyMilliSec < lastMilliSec || keyMilliSec === lastMilliSec && keySeq <= lastSeq) {
            conn.write(toSimpleError('ERR The ID specified in XADD is equal or smaller than the target stream top item'))
            return false
        }
        return true
    }

    autoGenEntryId(key) {

        function idSort(a, b) {
            return parseInt(a.split('-')[1]) > parseInt(b.split('-')[1])
        }

        if(key === '*') {
            return `${Date.now()}-0`
        }

        if(key.split('-')[1] === '*') {
            const keyMilliSec = parseInt(key.split('-')[0])
            const keys = Object.keys(this.streamData).filter(id => parseInt(id.split('-')[0]) === keyMilliSec).sort(idSort)
            if(keys.length) {
                return `${keys[keys.length-1].split('-')[0]}-${parseInt(keys[keys.length-1].split('-')[1])+1}`
            } else {
                return `${key.split('-')[0]}-${parseInt(key.split('-')[0]) === 0 ? 1 : 0}`
            }
        }

        return key
    }

    handleXADD(xAddArgs, conn) {
        if(this.validateXAdd(xAddArgs[1], conn)) {

            this.streamName = xAddArgs[0]
            const id = this.autoGenEntryId(xAddArgs[1])
            this.streamData[id] = { [xAddArgs[2]]: xAddArgs[3] }
            this.lastEntryKey = id
            conn.write(toBulkString(id))
        }
    }

    handleXRange(xRangeArgs, conn) {

        function filterRange(startId, endId, id) {

            if(startId === '-') {
                if(parseInt(id.split('-')[0]) <= parseInt(endId.split('-')[0]) && parseInt(id.split('-')[1]) <= parseInt(endId.split('-')[1])) {
                    return true
                } 
                return false
            }

            if(parseInt(id.split('-')[0]) >= parseInt(startId.split('-')[0]) && parseInt(id.split('-')[1]) >= parseInt(startId.split('-')[1]) && parseInt(id.split('-')[0]) <= parseInt(endId.split('-')[0]) && parseInt(id.split('-')[1]) <= parseInt(endId.split('-')[1])) {
                return true
            }
            return false
        }

        

        if(xRangeArgs[0] === this.streamName) {
            const startId = xRangeArgs[1]
            const endId = xRangeArgs[2]
            const keys = Object.keys(this.streamData).filter(id => filterRange(startId, endId, id))
            conn.write(this.formatInnerKeys(keys))
        }
    }

    formatInnerKeys(keys) {
        let resp = ''
        for(let key of keys) {
            resp = resp + `${objectToArray({ [key]: this.streamData[key]})}`
        }
        return `*${keys.length}\r\n${resp}`
    }
}



function objectToArray(obj) {
    let resp = ''
    for(let key of Object.keys(obj)) {
        resp = resp + `$${key.length}\r\n${key}\r\n`
        if(typeof obj[key] === 'object') {
            resp = resp + objectToArray(obj[key])
        } else {
            resp = resp + `$${obj[key].length}\r\n${obj[key]}\r\n`
        }
    }
    return `*${Object.keys(obj).length*2}\r\n${resp}`
}

function getStream() {
    if(!stream) {
        stream = new RedisStream()
    }
    return stream
}

module.exports = { getStream, RedisStream }