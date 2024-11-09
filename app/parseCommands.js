function parsedCommands(data) {
  let tokens = data.toString("utf-8").split("\r\n");
  if (!tokens[0].startsWith("*")) {
    tokens = data
      .toString("utf-8")
      .slice(data.toString("utf-8").indexOf("*"))
      .split("\r\n");
  }
  const commands = [];
  let currString = "";
  for (let i = 0; i < tokens.length; i++) {
    if (tokens[i].startsWith("*")) {
      commands.push("");
    } else if (tokens[i].startsWith("$")) {
    } else {
      currString = commands[commands.length - 1];
      currString = currString + ` ${tokens[i]}`;
      commands.push(currString.trim());
    }
  }
  return commands;
}

function toRespSimpleString(string) {
  return `+${string}\r\n`;
}

function toBulkString(string) {
  return `$${string.length}\r\n${string}\r\n`;
}

function toArray(array) {
  let resp = "";
  for (const elem of array) {
    resp = resp + `$${elem.length}\r\n${elem}\r\n`;
  }
  return `*${array.length}\r\n${resp}`;
}

function separateTCPSegment(data) {
  const utfString = data.toString("utf-8");
  let separatedUtfString = "";
  for (let i = 0; i < utfString.length; i++) {
    if (utfString[i] === "*") {
      if (utfString.slice(i + 1, i + 5) !== "\r\n" && i !== 0) {
        separatedUtfString = separatedUtfString + ",";
      }
    }
    separatedUtfString = separatedUtfString + utfString[i];
  }
  return separatedUtfString.split(',')
}

module.exports = {
  parsedCommands,
  toRespSimpleString,
  toBulkString,
  toArray,
  separateTCPSegment,
};
