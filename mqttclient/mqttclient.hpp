//
// Created by justnik on 15.08.22.
//

#ifndef MQTT_CLIENT_MQTTCLIENT_HPP
#define MQTT_CLIENT_MQTTCLIENT_HPP


#include <boost/array.hpp>
#include <boost/asio/io_service.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/write.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/signal_set.hpp>
#include <boost/asio/write.hpp>
#include <boost/asio/read.hpp>
#include <cstdio>
#include <string>
#include <thread>

using std::string;
using boost::asio::ip::tcp;
using boost::asio::awaitable;
using boost::asio::co_spawn;
using boost::asio::detached;
using boost::asio::use_awaitable;
#define SMCBUFSIZE 1024

class Mqtt_client {
private:
    const uint8_t CONNECT = 1 << 4,
            CONNACK = 2 << 4,
            PUBLISH = 3 << 4,
            PUBDUP = 3 << 4 | 8,
            PUBACK = 4 << 4,
            SUBSCRIBE = (8 << 4) | 2,
            SUBACK = 9 << 4,
            UNSUBSCRIBE = (10 << 4) | 2,
            UNSUBACK = 11 << 4,
            PINGREQ_2[2] = {12 << 4, 0},
            PINGRESP_2[2] = {13 << 4, 0}, //TODO: реализовать поддержку
    DISCONNECT_2[2] = {14 << 4, 0}; //TODO: реализовать поддержку
    //CONNECT_7[7] = {0x00, 0x04, 'M', 'Q', 'T', 'T', 0x04};


    const uint16_t KEEPALIVE_S = 60, PING_TIMEOUT = 15000;
    const uint32_t READ_TIMEOUT = 10000;
    uint16_t try_num = 0;
    const uint16_t max_try = 100;
    uint16_t port = 1883;
    uint8_t buffer[SMCBUFSIZE];

    std::atomic<bool> last_pub_acked = true;
    std::atomic<bool> last_conn_acked = false;
    std::atomic<bool> last_sub_acked = false;
    std::atomic<uint16_t> msg_id = 1, pubacked_msg_id = 0;

    boost::asio::io_service ioc;
    boost::asio::ip::tcp::socket socket = boost::asio::ip::tcp::socket(ioc);
    boost::asio::ip::tcp::socket socket_in = boost::asio::ip::tcp::socket(ioc);
    boost::asio::ip::tcp::endpoint endpoint;

    static const char *next_topic(const char *p);

    // Write a header into the start of the buffer and return the number of bytes written
    static uint16_t put_header(uint8_t header, uint8_t *buf, uint16_t len);

    static uint16_t put_string(const char *text, uint16_t len, uint8_t *buf, uint16_t pos);

    static uint16_t put_string(const char *text, uint8_t *buf, uint16_t pos);

    bool send_pingreq();

    bool publish(const char *topic, const char *payload, uint16_t payloadlen, bool retain = false,
                 uint8_t qos = 0);

    void suback_handler(const uint8_t *buf, uint16_t packet_len, bool unsubscribe);

    bool write_to_socket(size_t len);

    bool write_to_socket(const uint8_t *buf, uint16_t len);

    const int max_length = 1024;

    void read();

    awaitable<void> read_from_socket();

    bool puback_handler(const char8_t *reply, size_t len);

    bool connack_handler();

    bool wait_puback(uint16_t timeout_ms = 100);

    bool wait_suback(uint16_t timeout_ms = 100);

    bool wait_connack(uint16_t timeout_ms = 5000);

    void message_handler(char8_t *reply, size_t len);

public:

    //user callbacks
    std::function<void(const string &, const string &)> on_message = nullptr;

    explicit Mqtt_client(const string &ip = "127.0.0.1", uint16_t port = 1883);

    bool connect();

    bool publish(const char *topic, const char *payload, bool retain, uint8_t qos = 0);

    bool subscribe(const char *topic, uint8_t qos = 0, bool unsubscribe = false);
};


#endif //MQTT_CLIENT_MQTTCLIENT_HPP
