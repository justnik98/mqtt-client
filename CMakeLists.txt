cmake_minimum_required(VERSION 3.22)
project(mqtt_client)

set(CMAKE_CXX_STANDARD 20)
add_executable(mqtt_client main.cpp mqttclient/mqttclient.cpp mqttclient/mqttclient.hpp)