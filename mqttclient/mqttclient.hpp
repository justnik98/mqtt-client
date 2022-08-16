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
            PINGRESP_2[2] = {13 << 4, 0},
            DISCONNECT_2[2] = {14 << 4, 0},
            CONNECT_7[7] = {0x00, 0x04, 'M', 'Q', 'T', 'T', 0x04};


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

    const char *next_topic(const char *p) {
        while (*p && *p != ',') p++;
        return p;
    }

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

    bool send_pingreq() { write_to_socket(PINGREQ_2, 2); }

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
            else if (!wait_puback(100)) {
                if (try_num < max_try) {
                    publish(topic, payload, retain, qos);
                } else {
                    throw (std::runtime_error("Connection refused!"));
                }
            }
        }
        catch (std::exception &e) {
            std::cerr << e.what();
        }
        return ok;
    }

    void suback_handler(const uint8_t *buf, const uint16_t packet_len, bool unsubscribe) {
        if (packet_len == (unsubscribe ? 4 : 5) && buf[0] == (unsubscribe ? UNSUBACK : SUBACK)) {
            uint16_t mess_id = (buf[2] << 8) | buf[3];
            if (!unsubscribe && buf[4] > 2) return; // Return code indicates failure
            if (mess_id == msg_id) {
                last_sub_acked = true;
                try_num = 0;
            }
        }
    }

    bool write_to_socket(size_t len) {
        auto b = boost::asio::buffer(buffer, len);
        boost::system::error_code err;
        socket.write_some(b, err);
        return true;
    }

    bool write_to_socket(const uint8_t *buf, const uint16_t len) {
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

            if (reply[0] == PUBACK) {
                puback_handler(reinterpret_cast<char8_t *>(reply), n);
            } else if ((reply[0] & PUBLISH) == PUBLISH) {
                message_handler(reply, n);
            } else if (reply[0] == SUBACK) {
                suback_handler(reinterpret_cast<const uint8_t *>(reply), n, false);
            } else if (reply[0] == UNSUBACK) {
                suback_handler(reinterpret_cast<const uint8_t *>(reply), n, true);
            } else if (reply[0] == CONNACK) {
                connack_handler();
            }
        }
    }

    bool puback_handler(const char8_t *reply, size_t len) {
        pubacked_msg_id = (reply[2] << 8) | reply[3];
        if (pubacked_msg_id == msg_id) {
            last_pub_acked = true;
            try_num = 0;
        }
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

    bool wait_suback(uint16_t timeout_ms = 100) {
        using millis = std::chrono::duration<long, std::milli>;
        auto start = std::chrono::system_clock::now();
        while (!last_sub_acked &&
               (std::chrono::duration_cast<millis>(std::chrono::system_clock::now() - start).count() < timeout_ms)) {
        }
        return last_sub_acked;
    }

    bool wait_connack(uint16_t timeout_ms = 5000) {

        using millis = std::chrono::duration<long, std::milli>;
        auto start = std::chrono::system_clock::now();
        while (!last_conn_acked &&
               (std::chrono::duration_cast<millis>(std::chrono::system_clock::now() - start).count() < timeout_ms)) {
        }
        return last_conn_acked;
    }

    void message_handler(char8_t *reply, size_t len) {
        string rep((char *) reply, len);
        uint16_t topic_len = reply[2] << 8 | reply[3];
        string topic = rep.substr(4, topic_len);
        string msg = rep.substr(4 + topic_len);
        on_message(topic, msg);
    }

public:

    //user callbacks
    std::function<void(const string&, const string&)> on_message = nullptr;

    Mqtt_client(const string &ip = "127.0.0.1", uint16_t port = 1883): port(port) {
        endpoint.address(boost::asio::ip::address::from_string(ip));
        endpoint.port(port);
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

    bool subscribe(const char *topic, const uint8_t qos, bool unsubscribe = false) {
        last_sub_acked = false;
        uint16_t payload_len = 2; // packet identifier
        const char *p = topic, *p2 = p;
        while (*p && (p2 = next_topic(p))) { // Find next comma or final null-terminator
            payload_len += ((uint16_t) (p2 - p) + 2) + (unsubscribe ? 0 : 1);
            p = *p2 ? p2 + 1 : p2; // Skip comma if several topics are listed
        }
        // Add header and packet identifier
        uint16_t len = put_header(unsubscribe ? UNSUBSCRIBE : SUBSCRIBE, buffer, payload_len);
        if (++msg_id == 0) msg_id++; // Avoid 0
        buffer[len++] = msg_id >> 8;
        buffer[len++] = msg_id & 0xFF;
        // Add each topic
        p = topic;
        while (*p && (p2 = next_topic(p))) {
            payload_len += ((uint16_t) (p2 - p) + 2) + (unsubscribe ? 0 : 1);
            len += put_string(p, (uint16_t) (p2 - p), buffer, len);
            if (!unsubscribe) buffer[len++] = qos;
            p = *p2 ? p2 + 1 : p2; // Skip comma if several topics are listed
        }
        write_to_socket(len);
        if (!wait_suback(100)) {
            if (try_num < max_try) {
                subscribe(topic, qos, unsubscribe);
            } else {
                throw (std::runtime_error("Connection refused!"));
            }
        }
    }
};


#endif //MQTT_CLIENT_MQTTCLIENT_HPP
