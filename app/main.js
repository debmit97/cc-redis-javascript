const net = require("net");

// Uncomment this block to pass the first stage
const server = net.createServer((connection) => {
    connection.on('data', (data) => {
        if (data.toString() === '*1\r\n$4\r\nPING\r\n') {
          connection.write('+PONG\r\n');
        }
       
      });
});

server.listen(6379, "127.0.0.1");
