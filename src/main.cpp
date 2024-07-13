#include "include/RedisServer.hpp"

int main(int argc, char *argv[])
{
  // Flush after every std::cout / std::cerr
  std::cout << std::unitbuf;
  std::cerr << std::unitbuf;

  // Check if any arguments are provided
  if (argc > 1)
  {
    std::cout << "Arguments passed to the program:" << std::endl;
    for (int i = 1; i < argc; ++i)
    {
      std::cout << "Argument " << i << ": " << argv[i] << std::endl;
    }
  }
  else
  {
    std::cout << "No arguments were provided." << std::endl;
  }

  // Example of using a specific argument, e.g., --port
  std::string port;
  for (int i = 1; i < argc; ++i)
  {
    std::string arg = argv[i];
    if (arg == "--port" && i + 1 < argc)
    {
      port = argv[i + 1];
      break;
    }
  }

  // Start the Redis Server
  try
  {
    if (!port.empty())
    {
      std::cout << "Port specified: " << port << std::endl;
      RedisServer redisServer = RedisServer(std::stoi(port));
    }
    else
    {
      std::cout << "Port not specified or invalid." << std::endl;
      RedisServer redisServer = RedisServer();
    }
  }
  catch (const std::exception &e)
  {
    std::cerr << "Exception: " << e.what() << "\n";
    return EXIT_FAILURE;
  }

  return EXIT_SUCCESS;
}
