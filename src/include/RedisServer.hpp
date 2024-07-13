// https://redis.io/docs/latest/develop/reference/protocol-spec/

#include <iostream>
#include <unistd.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <cstring>
#include <thread>
#include <unordered_map>
#include <chrono>
#include <cassert>
#include "resp/all.hpp" // Repo Link : https://github.com/nousxiong/resp

/// @brief Initialize and start a Redis Server
class RedisServer
{
public:
    RedisServer(int DEFAULT_PORT = 6379)
    {
        PORT = DEFAULT_PORT;
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
    std::string role;
    int BUFFER_SIZE = 4096;
    int PORT;
    int CONNECTION_BACKLOG = 5;
    int server_fd_ = -1;
    std::unordered_map<std::string, std::pair<std::string, std::chrono::steady_clock::time_point>> umap;

    void info(int fd, resp::unique_value &rep)
    {
        // TODO
    }

    void echo(int fd, resp::unique_value &rep)
    {
        if (rep.array().size() > 1 && rep.array()[1].type() == resp::ty_bulkstr)
        {
            std::string message = rep.array()[1].bulkstr().data();
            std::string response = "$" + std::to_string(message.length()) + "\r\n" + message + "\r\n";
            send(fd, response.c_str(), response.length(), 0);
        }
        else
        {
            std::string error_response = "-ERR wrong number of arguments for 'echo' command\r\n";
            send(fd, error_response.c_str(), error_response.length(), 0);
        }
    }

    void setValue(int fd, resp::unique_value &rep)
    {
        if (rep.array().size() >= 3 && rep.array()[1].type() == resp::ty_bulkstr)
        {
            std::string key = rep.array()[1].bulkstr().data();
            std::string value = rep.array()[2].bulkstr().data();
            if (rep.array().size() == 5 && strcasecmp(rep.array()[3].bulkstr().data(), "px") == 0 && rep.array()[4].type() == resp::ty_bulkstr)
            {
                int expiry_ms = std::stoi(rep.array()[4].bulkstr().data());
                auto expiry_time = std::chrono::steady_clock::now() + std::chrono::milliseconds(expiry_ms);
                umap[key] = {value, expiry_time};
            }
            else
            {
                umap[key] = {value, std::chrono::steady_clock::time_point::max()};
            }
            assert(umap[key].first == value);
            send(fd, "+OK\r\n", 5, 0);
        }
        else
        {
            std::string error_response = "-ERR wrong number of arguments for 'set' command\r\n";
            send(fd, error_response.c_str(), error_response.length(), 0);
        }
    }

    void getValue(int fd, resp::unique_value &rep)
    {
        if (rep.array().size() > 1 && rep.array()[1].type() == resp::ty_bulkstr)
        {
            std::string key = rep.array()[1].bulkstr().data();
            auto it = umap.find(key);
            if (it != umap.end() && it->second.second > std::chrono::steady_clock::now())
            {
                std::string message = it->second.first;
                std::string response = "$" + std::to_string(message.length()) + "\r\n" + message + "\r\n";
                send(fd, response.c_str(), response.length(), 0);
            }
            else
            {
                std::string error_response = "$-1\r\n";
                send(fd, error_response.c_str(), error_response.length(), 0);
                umap.erase(it);
            }
        }
        else
        {
            std::string error_response = "-ERR wrong number of arguments for 'get' command\r\n";
            send(fd, error_response.c_str(), error_response.length(), 0);
        }
    }

    /// @brief Processes Decoded commands from client request.
    /// @param fd
    /// @param command
    /// @param rep
    /// @return returns 0 on success, -1 on failure to process command.
    int processCommand(int fd, std::string command, resp::unique_value &rep)
    {
        if (strcasecmp(command.c_str(), "ping") == 0)
        {
            // Simple String in RESP : https://redis.io/docs/latest/develop/reference/protocol-spec/#simple-strings
            send(fd, "+PONG\r\n", 7, 0);
        }
        else if (strcasecmp(command.c_str(), "echo") == 0)
        {
            echo(fd, rep);
        }
        else if (strcasecmp(command.c_str(), "set") == 0)
        {
            setValue(fd, rep);
        }
        else if (strcasecmp(command.c_str(), "get") == 0)
        {
            getValue(fd, rep);
        }
        else
        {
            return -1;
        }
        return 0;
    }

    /// @brief Handle Incoming requests from clients in a seprate thread.
    /// @param fd connection on socket FD.
    void handleRequest(int fd)
    {
        char buff[BUFFER_SIZE] = "";

        // Handle multiple requests
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
                if (processCommand(fd, command, rep) < 0)
                {
                    break;
                }
            }
        }
    }

    /// @brief Initialze the server using socket and start accepting concurrent requests from clients.
    void initServer()
    {
        /*
         The steps involved in establishing a socket on the client side are as follows:
           * Create a socket with the socket() system call (client side)
             + Connect the socket to the address of the server using the connect() system call
             + Send and receive data. There are a number of ways to do this, but the simplest is to use the read() and write() system calls.
             + The steps involved in establishing a socket on the server side are as follows:

           * Create a socket with the socket() system call (server side)
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

        // Handle concurrent requests
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