// rbd file has the information in bynary, to know more about the content and format: https://rdb.fnordig.de/file_format.html
class RDBParser {
  REDIS_BYTES_SIZE = 5; // first 5 bytes of the rbd file are the word "REDIS"
  RBD_VERSION_BYTES_SIZE = 4; // next 4 bytes are the version number
  OPCodes = {
      // Auxiliary fields. Arbitrary key-value settings, is followed by two redis strings: the key and the value of a setting
      AUX: 0xFA, 
      // Hash table sizes for the main keyspace and expires
      RESIZEDB: 0xFB,
      // Expire time in milliseconds
      EXPIRETIMEMS: 0xFC,
      // Expire time in seconds
      EXPIRETIME: 0xFD,
      // Database Selector
      SELECTDB: 0xFE,
      // End of the RDB file
      EOF: 0xFF
  }
  LengthTypes = {
      length: "length",
      format: "format"
  }
  constructor(buffer) {
      this.buffer = buffer; // the buffer of the RDB file
      this.cursor = 0; // to indicate where we are reading the rbd file
      this. dataStore = new Map(); // MAP of: string (key) value: Object with value and expiration attributes
      this.auxData = {}; // To estore what comes in the AUX field
  }
  parse() {
      let REDIS = this.bytesToString(this.REDIS_BYTES_SIZE); // get the first 5 bytes of REDIS word
      let VERSION = this.bytesToString(this.RBD_VERSION_BYTES_SIZE); // get the followed 4 bytes of RDB version
      while(true) {
          const opCode = this.readByte(); // get next byte
          switch(opCode) {
              case this.OPCodes.AUX:
                  this.getAUXField();
                  break;
              case this.OPCodes.RESIZEDB:
                  this.readResizeDB();
                  break;
              case this.OPCodes.EXPIRETIMEMS:
                  this.readExpireTimeMS();
                  break;
              case this.OPCodes.EXPIRETIME:
                  this.readExpireTime();
                  break;
              case this.OPCodes.SELECTDB:
                  this.readSelectDB();
                  break;
              case this.OPCodes.EOF:
                  return;
              default:
                  this.readKeyWithoutExpiry();
                  break;
          }
      }
  }
  getAUXField() {
      // Have two Redis Strings, the first is the key and the second is the value
      const key = this.getRedisString();
      const value = this.getRedisString();
      this.auxData[key] = value;
  }
  readResizeDB(){
      // It encodes two values to speed up RDB loading by avoiding additional resizes and rehashing.
      let [hashTableType, hashTableSize] = this.readLengthEncoding();
      let [expireHashTableType, expireHashTableSize] = this.readLengthEncoding();
  }
  readExpireTimeMS(){
      // The following expire value is specified in milliseconds. The following 8 bytes represent the Unix timestamp as an unsigned long.
      let timestamp = this.read8Bytes();
      let valueType = this.readByte();
      let key = this.getRedisString();
      let value = this.getRedisString();
      // TODO: save it in dataStore
      this.dataStore.set(key, {
          value: value,
          expiration: timestamp
      });
  }
  readExpireTime(){
      // The following expire value is specified in seconds. The following 4 bytes represent the Unix timestamp as an unsigned integer.
      let timestamp = this.read4Bytes();
      let valueType = this.readByte();
      let key = this.getRedisString();
      let value = this.getRedisString();
      // TODO: add to the storage
      this.dataStore.set(key, {
          value: value,
          expiration: timestamp
      })
  }
  readSelectDB(){
      // A Redis instance can have multiple databases. Length field indicates the database number
      let [type, value] = this.readLengthEncoding();
  }
  readKeyWithoutExpiry(){
      // For this simplified case only, is not in the oficial doc
      let key = this.getRedisString();
      let value = this.getRedisString();
      this.dataStore.set(key, {value: value});
  }
  bytesToString(byteLength) {
      let string = String.fromCharCode(...(this.buffer.subarray(this.cursor, this.cursor + byteLength)));
      this.cursor +=byteLength;
      return string;
  }
  getRedisString() {
      // Redis Strings are like byte arrays, without any special end-of-string token.
      //There are three types of Strings in Redis:
      //      - Length prefixed strings (use Length Encoding to get the length)
      //      - An 8, 16 or 32 bit integer (if Length Encoding first two bits are '11', use the remaining bit to get the size of this)
      //      - A LZF compressed string (if Length Encoding first two bits are '11', and the remaining bits are 3 is this type)
      const [type, value] = this.readLengthEncoding();
      if(type === this.LengthTypes.length) { // first two bits are 00, 01 or 10
          return this.bytesToString(value); // given the lenght read the string
      } 
      if(value === 0){
          return `${this.readByte()}`;
      }
      else if(value === 1){
          return `${this.read2Bytes()}`;
      }
      else if(value === 2){
          return `${this.read4Bytes()}`;
      }
  }
  readLengthEncoding() {
      // get the first byte
      let firstByte = this.readByte();
      let twoBits = firstByte >> 6; // since we have a byte (8 bits) and we need to check the first 2 bits, shift 6 places (bits) to have a byte with only the 2 first bits
      let value = 0;
      let type = this.LengthTypes.length;
      if(twoBits === 0b00){ // if is 00, the next 6 bits represents the length // 0b prefix means binary
          value = firstByte & 0b00111111; // & to get the bit on in the 6 latest bits of the byte
      } 
      else if(twoBits === 0b01){ // if is 01, the next 6 bits with the next byte represents the length
          let secondByte = this.readByte();
          value = ((firstByte & 0b00111111) << 8) | (secondByte); // & to get the bit on in the 6 latest bits of the byte
          // shift to add a byte of 0s at the end and use the 'or' to fill that byte with the second byte.
      } 
      else if(twoBits === 0b10){ // if 10 means that the length is in the next 4 bytes
          value = this.read4Bytes();
      }
      else if(twoBits === 0b11){ // if 11, means a special format. This format is defined in the remaining 6 bits
          type = this.LengthTypes.format;
          value = firstByte & 0b00111111;
      }
      else{
          throw new Error(`Error while reading length encoding, got first byte as: ${firstByte}`);
      }
      return [type, value];
  }
  readByte() {
      return this.buffer[this.cursor++];
  }
  read2Bytes(){
      let bytes = this.buffer.readUInt16LE(this.cursor);
      this.cursor += 2;
      return bytes;
  }
  read4Bytes() {
      let bytes = this.buffer.readUInt32LE(this.cursor);
      this.cursor += 4;
      return bytes;
  }
  read8Bytes() {
      let bytes = this.buffer.readBigUint64LE(this.cursor);
      this.cursor += 8;
      return bytes;
  }
}
module.exports = {RDBParser};