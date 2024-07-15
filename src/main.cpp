#include "include/RedisServer.hpp"

int main(int argc, char *argv[])
{
  // Flush after every std::cout / std::cerr
  std::cout << std::unitbuf;
  std::cerr << std::unitbuf;

  server_metadata serv_meta;
  // Check if any arguments are provided
  // if (argc > 1)
  // {
  //   std::cout << "Arguments passed to the program:" << std::endl;
  //   for (int i = 1; i < argc; ++i)
  //   {
  //     std::cout << "Argument " << i << ": " << argv[i] << std::endl;
  //   }
  // }
  // else
  // {
  //   std::cout << "No arguments were provided." << std::endl;
  // }

  for (int i = 1; i < argc; ++i)
  {
    std::string arg = argv[i];
    if (arg == "--port" && i + 1 < argc)
    {
      serv_meta.port = std::stoi(argv[i + 1]);
    }
    else if (arg == "--replicaof" && i + 1 < argc)
    {
      serv_meta.master = argv[i + 1];
      serv_meta.is_replica = true;
    }
  }

  // Start the Redis Server
  try
  {
    RedisServer redisServer = RedisServer(serv_meta);
  }
  catch (const std::exception &e)
  {
    std::cerr << "Exception: " << e.what() << "\n";
    return EXIT_FAILURE;
  }

  return EXIT_SUCCESS;
}
