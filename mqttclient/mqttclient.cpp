//
// Created by justnik on 15.08.22.
//

#include "mqttclient.hpp"
#include <iostream>

const char *Mqtt_client::next_topic(const char *p) {
    while (*p && *p != ',') p++;
    return p;
}

uint16_t Mqtt_client::put_header(const uint8_t header, uint8_t *buf, const uint16_t len) {
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

uint16_t Mqtt_client::put_string(const char *text, uint16_t len, uint8_t *buf, const uint16_t pos) {
    uint16_t p = pos;
    if (len == 0) return 0;
    buf[p++] = len >> 8;
    buf[p++] = len & 0xFF;
    memcpy(&buf[p], text, len);
    return 2 + len;
}

uint16_t Mqtt_client::put_string(const char *text, uint8_t *buf, const uint16_t pos) {
    return put_string(text, (uint16_t) strlen(text), buf, pos);
}

bool Mqtt_client::send_pingreq() { return write_to_socket(PINGREQ_2, 2); }

bool Mqtt_client::publish(const char *topic, const char *payload, const uint16_t payloadlen, const bool retain,
                          const uint8_t qos) {
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
        else if (!wait_puback()) {
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

void Mqtt_client::suback_handler(const uint8_t *buf, const uint16_t packet_len, bool unsubscribe) {
    if (packet_len == (unsubscribe ? 4 : 5) && buf[0] == (unsubscribe ? UNSUBACK : SUBACK)) {
        uint16_t mess_id = (buf[2] << 8) | buf[3];
        if (!unsubscribe && buf[4] > 2) return; // Return code indicates failure
        if (mess_id == msg_id) {
            last_sub_acked = true;
            try_num = 0;
        }
    }
}

bool Mqtt_client::write_to_socket(size_t len) {
    auto b = boost::asio::buffer(buffer, len);
    boost::system::error_code err;
    socket.write_some(b, err);
    return true;
}

bool Mqtt_client::write_to_socket(const uint8_t *buf, const uint16_t len) {
    auto b = boost::asio::buffer(buf, len);
    auto ok = socket.write_some(b);
    return ok;
}

void Mqtt_client::read() {
    boost::asio::signal_set signals(ioc, SIGINT, SIGTERM);
    signals.async_wait([&](auto, auto) { ioc.stop(); });
    co_spawn(ioc, read_from_socket(), detached);

    ioc.run();
}

awaitable<void> Mqtt_client::read_from_socket() {
    char8_t reply[1024*1024];
    while (true) {
        size_t n = co_await socket.async_receive(boost::asio::buffer(reply, max_length), use_awaitable);
//        for(auto i = 0; i < n; ++i){
//            std::cout << std::hex << int(reply[i]) << ' ';
//        }
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

bool Mqtt_client::puback_handler(const char8_t *reply, size_t len) {
    pubacked_msg_id = (reply[2] << 8) | reply[3];
    if (pubacked_msg_id == msg_id) {
        last_pub_acked = true;
        try_num = 0;
    }
    return last_pub_acked;
}

bool Mqtt_client::wait_puback(uint16_t timeout_ms) {
    using millis = std::chrono::duration<long, std::milli>;
    auto start = std::chrono::system_clock::now();
    while (!last_pub_acked &&
           (std::chrono::duration_cast<millis>(std::chrono::system_clock::now() - start).count() < 100)) {
    }
    return last_pub_acked;
}

bool Mqtt_client::wait_suback(uint16_t timeout_ms) {
    using millis = std::chrono::duration<long, std::milli>;
    auto start = std::chrono::system_clock::now();
    while (!last_sub_acked &&
           (std::chrono::duration_cast<millis>(std::chrono::system_clock::now() - start).count() < timeout_ms)) {
    }
    return last_sub_acked;
}

bool Mqtt_client::wait_connack(uint16_t timeout_ms) {

    using millis = std::chrono::duration<long, std::milli>;
    auto start = std::chrono::system_clock::now();
    while (!last_conn_acked &&
           (std::chrono::duration_cast<millis>(std::chrono::system_clock::now() - start).count() < timeout_ms)) {
    }
    return last_conn_acked;
}

void Mqtt_client::message_handler(char8_t *reply, size_t len) {
    string rep((char *) reply, len);
    uint16_t qos = (reply[0] & 0x07) >> 1;
    uint16_t payload_start = 2+  ((len > 0x80)?1:0);
    uint16_t topic_len = reply[payload_start] << 8 | reply[payload_start + 1];
    uint16_t header_len = payload_start + 2;
    string topic = rep.substr(header_len, topic_len);
    string msg = rep.substr(header_len + (qos ? 2 : 0) + topic_len);

    uint16_t rec_msg_id[] = {reply[header_len + topic_len], reply[header_len + topic_len + 1]};
    if (qos == 1) {
        buffer[0] = PUBACK;
        buffer[1] = 0x2;
        buffer[2] = rec_msg_id[0];
        buffer[3] = rec_msg_id[1];
        write_to_socket(4);
    }
    on_message(topic, msg);
}

Mqtt_client::Mqtt_client(const string &ip, uint16_t port) : port(port) {
    endpoint.address(boost::asio::ip::address::from_string(ip));
    endpoint.port(port);
    std::thread([&]() { read(); }).detach();
    std::thread([&]() {
        while (true) {
            std::this_thread::sleep_for(std::chrono::milliseconds(PING_TIMEOUT));
            send_pingreq();
        }
    }).detach();
    socket.connect(endpoint);
    std::cout << socket.local_endpoint() << std::endl;
    signal(SIGPIPE, SIG_IGN);
}

bool Mqtt_client::connect() {
    uint8_t msg[] = {CONNECT, 0x11, 0x00, 0x04, 'M', 'Q', 'T', 'T', 0x04, 0x02, 0x00, 0x3c, 0x00, 0x05, 'P', 'Q', 'R',
                     'S', 'T'};
    std::copy(msg, msg + sizeof(msg), buffer);
    write_to_socket(sizeof(msg));
    if (!wait_connack()) throw std::invalid_argument("ERROR");
    std::cerr << "connected\n";
    return last_conn_acked;
}

bool Mqtt_client::publish(const char *topic, const char *payload, const bool retain, const uint8_t qos) {
    return publish(topic, payload, (uint16_t) strlen(payload), retain, qos);
}

bool Mqtt_client::subscribe(const char *topic, const uint8_t qos, bool unsubscribe) {
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
    bool ok = write_to_socket(len);
    if (!wait_suback()) {
        if (try_num < max_try) {
            subscribe(topic, qos, unsubscribe);
        } else {
            throw (std::runtime_error("Connection refused!"));
        }
    }
    return ok;
}

bool Mqtt_client::subscribe(const char *topic, uint8_t qos) {
    return subscribe(topic, qos, false);
}

bool Mqtt_client::unsubscribe(const char *topic, uint8_t qos) {
    return subscribe(topic, qos, true);
}

bool Mqtt_client::connack_handler() {
    return last_conn_acked = true;
}

Mqtt_client::~Mqtt_client() {
    write_to_socket(DISCONNECT_2, 2);
}