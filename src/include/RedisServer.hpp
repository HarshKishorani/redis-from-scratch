// https://redis.io/docs/latest/develop/reference/protocol-spec/

#include <iostream>
#include <unistd.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <cstring>
#include <thread>
#include <bits/stdc++.h>
#include <chrono>
#include <cassert>
#include "resp/all.hpp" // Repo Link : https://github.com/nousxiong/resp

struct server_metadata
{
    int port = 6379;
    bool is_replica = false;
    std::string master;

    server_metadata() = default;
    server_metadata(int port, bool is_replica, std::string master) : port(port), is_replica(is_replica), master(master) {}
};

struct redisServerConfig
{
    std::string role = "master";
    int connected_slaves = 0;
    std::string master_replid = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb";
    int master_repl_offset = 0;
    int second_repl_offset = -1;
    int repl_backlog_active = 0;
    int repl_backlog_size = 1048576;
    int repl_backlog_first_byte_offset = 0;
    int repl_backlog_histlen = 0;

    redisServerConfig() = default;
};

/// @brief Initialize and start a Redis Server
class RedisServer
{
public:
    RedisServer(server_metadata server_meta = server_metadata()) : server_config{}
    {
        this->server_meta = server_meta;
        PORT = server_meta.port;
        if (server_meta.is_replica)
            server_config.role = "slave";
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
    redisServerConfig server_config;
    server_metadata server_meta;
    int BUFFER_SIZE = 4096;
    int PORT;
    int CONNECTION_BACKLOG = 5;
    int server_fd_ = -1;
    std::unordered_map<std::string, std::pair<std::string, std::chrono::steady_clock::time_point>> umap;
    std::unordered_set<int> connectedReplicas;

    void psync(int fd, resp::unique_value &rep)
    {
        std::string fullresync_response = "+FULLRESYNC " + server_config.master_replid + " 0\r\n";
        send(fd, fullresync_response.c_str(), fullresync_response.length(), 0);
        std::string hex_bytes = "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2";
        // RDB File Info : https://rdb.fnordig.de/file_format.html
        std::string empty_rdb = "";
        for (int i = 0; i < hex_bytes.length(); i += 2)
        {
            empty_rdb += static_cast<char>(std::stoi(hex_bytes.substr(i, 2), nullptr, 16));
        }
        fullresync_response = "$" + std::to_string(empty_rdb.length()) + "\r\n" + empty_rdb;
        send(fd, fullresync_response.c_str(), fullresync_response.length(), 0);

        // Store fd for proporgation of commands
        connectedReplicas.insert(fd);
    }

    void info(int fd, resp::unique_value &rep)
    {
        // Check if the command includes the replication section
        if (rep.array().size() > 1 && rep.array()[1].type() == resp::ty_bulkstr)
        {
            std::string section = rep.array()[1].bulkstr().data();
            if (strcasecmp(section.c_str(), "replication") == 0)
            {
                // Construct the response for the replication section
                std::string response =
                    "# Replication\r\n"
                    "role:" +
                    server_config.role + "\r\n" +
                    "connected_slaves:" + std::to_string(server_config.connected_slaves) + "\r\n" +
                    "master_replid:" + server_config.master_replid + "\r\n" +
                    "master_repl_offset:" + std::to_string(server_config.master_repl_offset) + "\r\n" +
                    "second_repl_offset:" + std::to_string(server_config.second_repl_offset) + "\r\n" +
                    "repl_backlog_active:" + std::to_string(server_config.repl_backlog_active) + "\r\n" +
                    "repl_backlog_size:" + std::to_string(server_config.repl_backlog_size) + "\r\n" +
                    "repl_backlog_first_byte_offset:" + std::to_string(server_config.repl_backlog_first_byte_offset) + "\r\n" +
                    "repl_backlog_histlen:" + std::to_string(server_config.repl_backlog_histlen) + "\r\n";

                std::string bulk_response = "$" + std::to_string(response.length()) + "\r\n" + response + "\r\n";
                send(fd, bulk_response.c_str(), bulk_response.length(), 0);
                return;
            }
        }

        // Default error response if the section is not supported
        std::string error_response = "-ERR unsupported INFO section\r\n";
        send(fd, error_response.c_str(), error_response.length(), 0);
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
            std::cout << "Setting value of " << key << " to " << value << std::endl;
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
            std::cout << "Set value of " << key << " to " << umap[key].first << std::endl;
            if (server_config.role == "master")
            {
                send(fd, "+OK\r\n", 5, 0);
                // Proporgate commands to replicas
                resp::encoder<resp::buffer> enc;
                std::vector<resp::buffer> buffers = enc.encode("SET", key, value);
                std::string message;
                for (auto it : buffers)
                {
                    message += it.data();
                }
                for (const int &replica : connectedReplicas)
                {
                    send(replica, message.c_str(), message.length(), 0);
                }
            }
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
                // umap.erase(it);
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
        else if (strcasecmp(command.c_str(), "info") == 0)
        {
            info(fd, rep);
        }
        // REPLCONF and PSYNC for Replica Master Handshake. See connect_master() function below for more information.
        else if (strcasecmp(command.c_str(), "REPLCONF") == 0)
        {
            send(fd, "+OK\r\n", 5, 0);
        }
        else if (strcasecmp(command.c_str(), "PSYNC") == 0)
        {
            psync(fd, rep);
        }
        else
        {
            return -1;
        }
        return 0;
    }

    /// @brief Handle Incoming requests from clients in a separate thread.
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

    /*
    When a replica connects to a master, it needs to go through a handshake process before receiving updates from the master.

    There are three parts to this handshake:
    - The replica sends a PING to the master.
    - The replica sends REPLCONF twice to the master.
    - The replica sends PSYNC to the master.
    */
    int connect_master()
    {
        char buffer[1024] = {0};
        std::string master = server_meta.master;
        if (master.empty())
        {
            std::cerr << "Master not found \n";
            std::exit(EXIT_FAILURE);
        }

        // Split the master string into host and port
        size_t space_pos = master.find(' ');
        std::string master_host = master.substr(0, space_pos);
        if (master_host == "localhost")
        {
            master_host = "127.0.0.1";
        }
        int master_port = std::stoi(master.substr(space_pos + 1));

        int master_fd = socket(AF_INET, SOCK_STREAM, 0);
        if (master_fd < 0)
        {
            std::cerr << "Failed to create socket for master connection\n";
            std::exit(EXIT_FAILURE);
        }

        struct sockaddr_in master_addr;
        master_addr.sin_family = AF_INET;
        master_addr.sin_port = htons(master_port);
        if (inet_pton(AF_INET, master_host.c_str(), &master_addr.sin_addr) <= 0)
        {
            std::cerr << "Invalid address/ Address not supported \n";
            std::exit(EXIT_FAILURE);
        }

        if (connect(master_fd, reinterpret_cast<struct sockaddr *>(&master_addr), sizeof(master_addr)) < 0)
        {
            std::cerr << "Connection to master failed\n";
            std::exit(EXIT_FAILURE);
        }

        // Step 1 : Send PING command as a RESP Array to the master and Receive PONG.
        std::string message = "*1\r\n$4\r\nPING\r\n";
        if (send(master_fd, message.c_str(), message.length(), 0) < 0)
        {
            std::cerr << "Failed to send PING command to master\n";
            close(master_fd);
            std::exit(EXIT_FAILURE);
        }

        // Receive PONG response
        if (recv(master_fd, buffer, sizeof(buffer), 0) < 0)
        {
            std::cerr << "Failed to receive PONG response from master\n";
            close(master_fd);
            std::exit(EXIT_FAILURE);
        }
        memset(buffer, 0, sizeof(buffer));

        // Step 2 : After receiving a response to PING, the replica then sends 2 REPLCONF commands to the master.
        // The REPLCONF command is used to configure replication. Replicas will send this command to the master twice:

        // The first time, it'll be sent like this: REPLCONF listening-port <PORT>
        // This is the replica notifying the master of the port it's listening on
        message = "*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$4\r\n6380\r\n";
        if (send(master_fd, message.c_str(), message.length(), 0) < 0)
        {
            std::cerr << "Failed to send PING command to master\n";
            close(master_fd);
            std::exit(EXIT_FAILURE);
        }
        if (recv(master_fd, buffer, sizeof(buffer), 0) < 0)
        {
            std::cerr << "Failed to receive PONG response from master\n";
            close(master_fd);
            std::exit(EXIT_FAILURE);
        }
        memset(buffer, 0, sizeof(buffer));

        // The second time, it'll be sent like this: REPLCONF capa psync2.
        // This is the replica notifying the master of its capabilities ("capa" is short for "capabilities")
        message = "*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n";
        if (send(master_fd, message.c_str(), message.length(), 0) < 0)
        {
            std::cerr << "Failed to send PING command to master\n";
            close(master_fd);
            std::exit(EXIT_FAILURE);
        }
        if (recv(master_fd, buffer, sizeof(buffer), 0) < 0)
        {
            std::cerr << "Failed to receive PONG response from master\n";
            close(master_fd);
            std::exit(EXIT_FAILURE);
        }
        memset(buffer, 0, sizeof(buffer));

        /*
        Step 3 :
        After receiving a response to the second REPLCONF, the replica then sends a PSYNC command to the master.

        The PSYNC command is used to synchronize the state of the replica with the master.
        The replica will send this command to the master with two arguments:

        1. The first argument is the replication ID of the master
            Since this is the first time the replica is connecting to the master, the replication ID will be ? (a question mark)
        2. The second argument is the offset of the master
            Since this is the first time the replica is connecting to the master, the offset will be -1
            So the final command sent will be PSYNC ? -1.

        The master will respond with a Simple string that looks like this:
        +FULLRESYNC <REPL_ID> 0\r\n
        */
        message = "*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n";
        if (send(master_fd, message.c_str(), message.length(), 0) < 0)
        {
            std::cerr << "Failed to send PING command to master\n";
            close(master_fd);
            std::exit(EXIT_FAILURE);
        }
        if (recv(master_fd, buffer, sizeof(buffer), 0) < 0)
        {
            std::cerr << "Failed to receive PONG response from master\n";
            close(master_fd);
            std::exit(EXIT_FAILURE);
        }
        memset(buffer, 0, sizeof(buffer));
        return master_fd;
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

        int master_fd = -1;
        if (server_config.role == "slave")
        {
            std::cout << "Connecting to master....." << server_meta.master << std::endl;
            master_fd = connect_master();
        }

        std::cout << "Waiting for a client to connect...\n";

        struct sockaddr_in client_addr;
        socklen_t client_addr_len = sizeof(client_addr);

        // Handle concurrent requests
        while (true)
        {
            if (master_fd != -1)
                std::thread(&RedisServer::handleRequest, this, master_fd).detach();
            int client_fd = accept(server_fd_, reinterpret_cast<struct sockaddr *>(&client_addr), &client_addr_len);
            if (client_fd < 0)
            {
                std::cerr << "Failed to accept client connection\n";
                std::exit(EXIT_FAILURE);
            }

            std::thread(&RedisServer::handleRequest, this, client_fd).detach(); // Handle concurrent clients using Threads
        }
    }
};