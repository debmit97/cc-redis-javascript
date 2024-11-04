const fs = require("fs");

const readRdbFile = (store) => {
  const opCodes = {
    resizeDb: "fb",
  };
  let i = 0;
  const dirName = process.argv[3];
  const fileName = process.argv[5];
  const filePath = dirName + "/" + fileName;
  let dataBuffer;
  try {
    dataBuffer = fs.readFileSync(filePath);
  } catch (e) {
    console.log("Error:", e);
    return;
  }
  console.log("Hex data:", dataBuffer.toString("hex"));

  /**
   * @param {int} n - No. Of bytes to get
   * @returns {Buffer} - Next n bytes
   * */
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

  const getKeyValues = (n) => {
    for (let j = 0; j < n; j++) {
      const keyLength = getNextObjLength();
      const key = getNextNBytes(keyLength).toString();
      const valueLength = getNextObjLength();
      const value = getNextNBytes(valueLength).toString();
      console.log(`Setting ${key} to ${value}`);
      store.set(key, value)
			i++; // 00 padding.
    }
  };

  const expiryHashTable = () => {
    const nextObjLength = getNextObjLength();
    const nextNBytes = getNextNBytes(nextObjLength);
  };

  const resizeDb = () => {
    console.log("Inside resizedb");
    i++;
    const totalKeyVal = getNextObjLength();
    const totalExpiry = getNextObjLength();
    i++; // There is 00 padding.
    getKeyValues(totalKeyVal);
  };

  while (i < dataBuffer.length) {
    const currentHexByte = dataBuffer[i].toString(16);
    if (currentHexByte === opCodes.resizeDb) resizeDb();
    i++;
  }

  return null;
};

module.exports = { readRdbFile };