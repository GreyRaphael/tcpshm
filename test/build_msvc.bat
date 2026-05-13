@echo off
cl /std:c++20 /O2 /EHsc /Fe:echo_server.exe echo_server.cc /link ws2_32.lib
cl /std:c++20 /O2 /EHsc /Fe:echo_client.exe echo_client.cc /link ws2_32.lib
