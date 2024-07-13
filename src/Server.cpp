#include <iostream>
#include <cstdlib>
#include <string>
#include <unistd.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <cstring>
#include <thread>
#include <vector>

constexpr int BUFFER_SIZE = 128;
constexpr int PORT = 6379;
constexpr int CONNECTION_BACKLOG = 5;

class RedisServer
{
public:
  RedisServer()
  {
    initServer();
  }

  ~RedisServer()
  {
    if (server_fd_ != -1)
    {
      close(server_fd_);
    }
  }

private:
  int server_fd_ = -1;

  void handleRequest(int fd)
  {
    std::string message = "+PONG\r\n";
    char buff[BUFFER_SIZE] = "";

    while (true)
    {
      memset(buff, 0, sizeof(buff));
      ssize_t bytes_received = recv(fd, buff, 15, 0);
      if (bytes_received <= 0)
      {
        std::cerr << "Failed to receive data or connection closed\n";
        close(fd);
        return;
      }

      if (strcasecmp(buff, "*1\r\n$4\r\nping\r\n") != 0)
      {
        memset(buff, 0, sizeof(buff));
        continue;
      }

      ssize_t bytes_sent = send(fd, message.c_str(), message.length(), 0);
      if (bytes_sent < 0)
      {
        std::cerr << "Failed to send response\n";
        close(fd);
        return;
      }
    }
  }

  void initServer()
  {
    /*
     The steps involved in establishing a socket on the client side are as follows:
       * Create a socket with the socket() system call
         + Connect the socket to the address of the server using the connect() system call
         + Send and receive data. There are a number of ways to do this, but the simplest is to use the read() and write() system calls.
         + The steps involved in establishing a socket on the server side are as follows:

       * Create a socket with the socket() system call
         + Bind the socket to an address using the bind() system call. For a server socket on the Internet, an address consists of a port number on the host machine.
         + Listen for connections with the listen() system call
         + Accept a connection with the accept() system call. This call typically blocks until a client connects with the server.
         + Send and receive data
    */

    server_fd_ = socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd_ < 0)
    {
      std::cerr << "Failed to create server socket\n";
      std::exit(EXIT_FAILURE);
    }

    int reuse = 1;
    if (setsockopt(server_fd_, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse)) < 0)
    {
      std::cerr << "setsockopt failed\n";
      std::exit(EXIT_FAILURE);
    }

    struct sockaddr_in server_addr;
    server_addr.sin_family = AF_INET;
    server_addr.sin_addr.s_addr = INADDR_ANY;
    server_addr.sin_port = htons(PORT);

    if (bind(server_fd_, reinterpret_cast<struct sockaddr *>(&server_addr), sizeof(server_addr)) != 0)
    {
      std::cerr << "Failed to bind to port " << PORT << "\n";
      std::exit(EXIT_FAILURE);
    }

    if (listen(server_fd_, CONNECTION_BACKLOG) != 0)
    {
      std::cerr << "listen failed\n";
      std::exit(EXIT_FAILURE);
    }

    std::cout << "Waiting for a client to connect...\n";

    struct sockaddr_in client_addr;
    socklen_t client_addr_len = sizeof(client_addr);

    while (true)
    {
      int client_fd = accept(server_fd_, reinterpret_cast<struct sockaddr *>(&client_addr), &client_addr_len);
      if (client_fd < 0)
      {
        std::cerr << "Failed to accept client connection\n";
        std::exit(EXIT_FAILURE);
      }

      std::cout << "Client connected. Handling Request\n";
      std::thread(&RedisServer::handleRequest, this, client_fd).detach(); // Handle concurrent clients using Threads
    }
  }
};

int main()
{
  // Flush after every std::cout / std::cerr
  std::cout << std::unitbuf;
  std::cerr << std::unitbuf;

  try
  {
    RedisServer redisServer;
  }
  catch (const std::exception &e)
  {
    std::cerr << "Exception: " << e.what() << "\n";
    return EXIT_FAILURE;
  }

  return EXIT_SUCCESS;
}
