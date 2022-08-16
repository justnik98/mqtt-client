//
// Created by justnik on 15.08.22.
//

#ifndef MQTT_CLIENT_MQTTCLIENT_HPP
#define MQTT_CLIENT_MQTTCLIENT_HPP

#include <string>
#include <boost/asio/io_service.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/array.hpp>
#include <boost/asio/write.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/signal_set.hpp>
#include <boost/asio/write.hpp>
#include <cstdio>
#include <thread>
#include <boost/asio/read.hpp>

using boost::asio::ip::tcp;
using boost::asio::awaitable;
using boost::asio::co_spawn;
using boost::asio::detached;
using boost::asio::use_awaitable;
#define SMCBUFSIZE 1000

auto count = 0l;

class Mqtt_client {
private:
    const uint8_t CONNECT = 1 << 4,
            CONNACK = 2 << 4,
            PUBLISH = 3 << 4,
            PUBDUP = 3 << 4 | 4,
            PUBACK = 4 << 4,
            SUBSCRIBE = (8 << 4) | 2,
            SUBACK = 9 << 4,
            UNSUBSCRIBE = (10 << 4) | 2,
            UNSUBACK = 11 << 4,
            PINGREQ_2[2] = {12 << 4, 0},
            PINGRESP_2[2] = {13 << 4, 0},
            DISCONNECT_2[2] = {14 << 4, 0},
            CONNECT_7[7] = {0x00, 0x04, 'M', 'Q', 'T', 'T', 0x04};
    const uint16_t KEEPALIVE_S = 60, PING_TIMEOUT = 15000;
    const uint32_t READ_TIMEOUT = 10000;
    uint16_t port = 1883;
    uint8_t buffer[SMCBUFSIZE];

    std::atomic<bool> last_pub_acked = true;
    std::atomic<bool> last_conn_acked = false;

    std::atomic<uint16_t> msg_id = 1, pubacked_msg_id = 0;

    boost::asio::io_service ioc;
    boost::asio::ip::tcp::socket socket = boost::asio::ip::tcp::socket(ioc);
    boost::asio::ip::tcp::socket socket_in = boost::asio::ip::tcp::socket(ioc);

    // Write a header into the start of the buffer and return the number of bytes written
    uint16_t put_header(const uint8_t header, uint8_t *buf, const uint16_t len) {
        uint16_t l = len;
        uint8_t pos = 0, v;
        buf[pos++] = header;
        do {
            v = l % 0x80;
            l /= 0x80;
            buf[pos++] = l > 0 ? v | 0x80 : v;
        } while (l != 0);
        return pos;
    }

    static uint16_t put_string(const char *text, uint16_t len, uint8_t *buf, const uint16_t pos) {
        uint16_t p = pos;
        if (len == 0) return 0;
        buf[p++] = len >> 8;
        buf[p++] = len & 0xFF;
        memcpy(&buf[p], text, len);
        return 2 + len;
    }

    uint16_t put_string(const char *text, uint8_t *buf, const uint16_t pos) {
        return put_string(text, (uint16_t) strlen(text), buf, pos);
    }

    bool publish(const char *topic, const char *payload, const uint16_t payloadlen, const bool retain = false,
                 const uint8_t qos = 0) {
        auto len = 0;
        bool ok = false;
        try {

            uint16_t total = ((uint16_t) strlen(topic) + 2) + (qos > 0 ? 2 : 0) + payloadlen + 2;
            if (last_pub_acked) {
                last_pub_acked = false;
                len = put_header(PUBLISH, buffer, total);
            } else {
                len = put_header(PUBDUP, buffer, total);
            }
            if (retain) buffer[0] |= 1;
            buffer[0] |= qos << 1; // Add QOS into second or third bit
            len += put_string(topic, buffer, len);
            if (qos > 0) {
                if (++msg_id == 0) msg_id++;
                buffer[len++] = msg_id >> 8;
                buffer[len++] = msg_id & 0xFF;
            }
            //memcpy(&buffer[len], payload, payloadlen);
            len += put_string(payload, buffer, len);
            ok = write_to_socket(len);
            if (qos == 0) last_pub_acked = ok;
            else if (!wait_puback(1)) publish(topic, payload, retain, qos);
        }
        catch (boost::wrapexcept<boost::system::system_error> &e) {

            std::cerr << e.what();
        }
//        catch (std::exception &e) {
//        }
        return ok;
    }

    bool write_to_socket(size_t len) {
        std::array<char, 64> buf{};
        std::copy(buffer, buffer + len, buf.begin());
        auto b = boost::asio::buffer(buf, len);

        auto ok = boost::asio::write(socket, b);

        return ok;
    }

    const int max_length = 1024;

    void read() {
        boost::asio::signal_set signals(ioc, SIGINT, SIGTERM);
        signals.async_wait([&](auto, auto) { ioc.stop(); });
        co_spawn(ioc, read_from_socket(), detached);

        ioc.run();
    }

    awaitable<void> read_from_socket() {
        char8_t reply[1024];
        while (true) {
            auto payload_len = 0;

            size_t n = co_await socket.async_receive(boost::asio::buffer(reply, max_length), use_awaitable);
            std::cout << "Reply is: ";
            for (auto i = 0; i < n; ++i) {
                std::cout << std::hex << (int) reply[i];
            }
            std::cout << std::endl;
            if (reply[0] == CONNACK) {
                connack_handler();
            }
            if (reply[0] == PUBACK) {
                puback_handler(reinterpret_cast<char8_t *>(reply), n);
            }

        }
    }

    bool puback_handler(char8_t *reply, size_t len) {
        auto payload_len = 0;
        payload_len = reply[1] & 0x7F;
        pubacked_msg_id = (reply[2] << 8) | reply[3];
        if (pubacked_msg_id == msg_id) last_pub_acked = true;
        for (auto i = 0; i < payload_len + 2; ++i) {
            // std::cout << std::hex << (int) reply[i];
        }
        // std::cout << ' ' << payload_len << std::dec << "\n";
        return last_pub_acked;
    }

    bool connack_handler() {
        return last_conn_acked = true;
    }

    bool wait_puback(uint16_t timeout_ms = 100) {
        using millis = std::chrono::duration<long, std::milli>;
        auto start = std::chrono::system_clock::now();
        while (!last_pub_acked &&
               (std::chrono::duration_cast<millis>(std::chrono::system_clock::now() - start).count() < timeout_ms)) {
        }
        return last_pub_acked;
    }

    bool wait_connack(uint16_t timeout_ms = 3000) {

        using millis = std::chrono::duration<long, std::milli>;
        auto start = std::chrono::system_clock::now();
        while (!last_conn_acked &&
               (std::chrono::duration_cast<millis>(std::chrono::system_clock::now() - start).count() < timeout_ms)) {
        }
        return last_conn_acked;
    }

public:

    Mqtt_client() {
        boost::asio::ip::tcp::endpoint endpoint(boost::asio::ip::address::from_string("127.0.0.1"), port);
        std::thread([&]() { read(); }).detach();
        socket.connect(endpoint);
        std::cout << socket.local_endpoint() << std::endl;
        signal(SIGPIPE, SIG_IGN);
    }

    bool connect() {
        char msg[] = {0x10, 0x11, 0, 4, 'M', 'Q', 'T', 'T', 4, 2, 0, 0x3c, 0, 5, 'P', 'Q', 'R', 'S', 'T'};
        std::copy(msg, msg + sizeof(msg), buffer);
        write_to_socket(sizeof(msg));
        if (!wait_connack()) throw std::invalid_argument("ERROR");
        return last_conn_acked;
    }

    bool publish(const char *topic, const char *payload, const bool retain, const uint8_t qos = 0) {
        return publish(topic, payload, (uint16_t) strlen(payload), retain, qos);
    }

};


#endif //MQTT_CLIENT_MQTTCLIENT_HPP
