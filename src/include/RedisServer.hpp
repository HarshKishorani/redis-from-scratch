#include <iostream>
#include <unistd.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <cstring>
#include <thread>
#include "resp/all.hpp" // Repo Link : https://github.com/nousxiong/resp
#include <unordered_map>
#include <cassert>

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
    int BUFFER_SIZE = 4096;
    int PORT = 6379;
    int CONNECTION_BACKLOG = 5;
    int server_fd_ = -1;
    std::unordered_map<std::string, std::string> umap;

    void handleRequest(int fd)
    {
        // This is Redis Simple String for PONG reply to Ping
        char buff[BUFFER_SIZE] = "";

        while (true)
        {
            resp::decoder dec;
            memset(buff, 0, sizeof(buff));
            ssize_t bytes_received = recv(fd, buff, sizeof(buff), 0); // receive from client
            if (bytes_received <= 0)
            {
                return;
            }

            resp::result request = dec.decode(buff, std::strlen(buff));
            resp::unique_value rep = request.value();
            if ((rep.type() == resp::ty_array) && (rep.array()[0].type() == resp::ty_bulkstr))
            {
                std::string command = rep.array()[0].bulkstr().data();
                if (strcasecmp(command.c_str(), "ping") == 0)
                {
                    // Simple String in RESP : https://redis.io/docs/latest/develop/reference/protocol-spec/#simple-strings
                    send(fd, "+PONG\r\n", 7, 0);
                }
                else if (strcasecmp(command.c_str(), "echo") == 0)
                {
                    if (rep.array().size() > 1 && rep.array()[1].type() == resp::ty_bulkstr)
                    {
                        std::string message = rep.array()[1].bulkstr().data();
                        std::string response = "$" + std::to_string(message.length()) + "\r\n" + message + "\r\n";
                        send(fd, response.c_str(), response.length(), 0);
                    }
                    else
                    {
                        // Handle error: echo command without a message
                        std::string error_response = "-ERR wrong number of arguments for 'echo' command\r\n";
                        send(fd, error_response.c_str(), error_response.length(), 0);
                    }
                }
                else if (strcasecmp(command.c_str(), "set") == 0)
                {
                    if (rep.array().size() > 2 && rep.array()[1].type() == resp::ty_bulkstr)
                    {
                        std::string key = rep.array()[1].bulkstr().data();
                        std::string value = rep.array()[2].bulkstr().data();
                        umap[key] = value;
                        assert(umap[key] == value);
                        send(fd, "+OK\r\n", 5, 0);
                    }
                    else
                    {
                        // Handle error: echo command without a message
                        std::string error_response = "-ERR wrong number of arguments for 'set' command\r\n";
                        send(fd, error_response.c_str(), error_response.length(), 0);
                    }
                }
                else if (strcasecmp(command.c_str(), "GET") == 0)
                {
                    if (rep.array().size() > 1 && rep.array()[1].type() == resp::ty_bulkstr)
                    {
                        std::string key = rep.array()[1].bulkstr().data();
                        std::string message = umap[key];
                        std::string response = "$" + std::to_string(message.length()) + "\r\n" + message + "\r\n";
                        send(fd, response.c_str(), response.length(), 0);
                    }
                    else
                    {
                        // Handle error: echo command without a message
                        std::string error_response = "-ERR wrong number of arguments for 'get' command\r\n";
                        send(fd, error_response.c_str(), error_response.length(), 0);
                    }
                }
                else
                {
                    break;
                }
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

        // Server Side setup
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