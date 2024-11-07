function parsedCommands(data) {
  let tokens = data.toString("utf-8").split("\r\n");
  if(!tokens[0].startsWith('*')) {
    tokens = data.toString("utf-8").slice(data.toString("utf-8").indexOf('*')).split('\r\n')
  }
  const commands = [];
  let currString = ''
  for (let i = 0; i < tokens.length; i++) {
    if(tokens[i].startsWith('*')) {
        commands.push('')
    } else if(tokens[i].startsWith('$')) {

    } else {
        currString = commands[commands.length-1]
        currString = currString+` ${tokens[i]}`
        commands.push(currString.trim())
    }
  }
  return commands
}

module.exports = { parsedCommands };
