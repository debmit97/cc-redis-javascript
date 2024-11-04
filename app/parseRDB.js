const fs = require("fs");

const db = {}

//Reads RDB File
const readRdbFile = (store) => {
  const opCodes = {
    resizeDb: "fb",
  };
  let i = 0;
  const dirName = process.argv[3];
  const fileName = process.argv[5];
  const filePath = dirName + "/" + fileName;
  console.log(`DIr: ${dirName} ,Filename :${fileName}`);
  console.log(`Path`, filePath);
  const dataBuffer = fs.readFileSync(filePath);
  console.log("Hex data:", dataBuffer.toString("hex"));
  const getNextNBytes = (n) => {
    let nextNBytes = Buffer.alloc(n);
    for (let k = 0; k < n; k++) {
      nextNBytes[k] = dataBuffer[i];
      i++;
    }
    return nextNBytes;
  };
  const getNextObjLength = () => {
    const firstByte = dataBuffer[i];
    const twoBits = firstByte >> 6;
    let length = 0;
    switch (twoBits) {
      case 0b00:
        length = firstByte ^ 0b00000000;
        i++;
        break;
    }
    return length;
  };
  const hashTable = () => {
    const nextObjLength = getNextObjLength();
    const nextNBytes = getNextNBytes(nextObjLength);
  };
  const expiryHashTable = () => {
    const nextObjLength = getNextObjLength();
    const nextNBytes = getNextNBytes(nextObjLength);
  };
  const resizeDb = () => {
    console.log("Inside resizedb");
    i++;
    hashTable();
    expiryHashTable();
    const keyLength = getNextObjLength();
    const key = getNextNBytes(keyLength);
    const valueLength = getNextObjLength();
    const value = getNextNBytes(valueLength);
    console.log("Key:", key.toString(), "value:", value.toString());
    store.set(key.toString(), value.toString());
  };
  while (i < dataBuffer.length) {
    const currentHexByte = dataBuffer[i].toString(16);
    if (currentHexByte === opCodes.resizeDb) resizeDb();
    i++;
  }
}

module.exports = { readRdbFile };
