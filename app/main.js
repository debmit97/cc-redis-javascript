const net = require("net");

// Uncomment this block to pass the first stage
const server = net.createServer((connection) => {
  connection.write('+PONG\r\n')
});

server.listen(6379, "127.0.0.1");
