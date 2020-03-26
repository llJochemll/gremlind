module gremlind.client;

import std.conv;
import std.string;
import std.uuid;
import vibe.core.concurrency : async;
import vibe.core.core;
import vibe.core.connectionpool;
import vibe.data.json;
import vibe.http.status;
import vibe.http.websockets;
import vibe.inet.url;

@safe
class GremlinClient {
    this(URL url, string user, string password) {
        m_url = url;
        m_user = user;
        m_password = password;

        openConnection;
    }

    this(string url, string user, string password) {
        this(URL(url), user, password);
    }

    void openConnection() {
        if (m_connection !is null) {
            m_connection.close;
        }
        
        m_connection = new GremlinConnection(m_url, m_user, m_password);
    }

    auto query(string queryString) {
        return queryAsync(queryString).getResult;
    }

    auto queryAsync(string queryString) {
        return m_connection.query(queryString);
    }

    private {
        URL m_url;
        string m_user;
        string m_password;
        GremlinConnection m_connection;
    }
}

@safe
class GremlinClientResponse {
    Response.Status status;
    Json data;

    this() {
        data = Json.emptyArray;
    }
}

@safe
private class GremlinConnection {
    URL m_url;
    string m_user;
    string m_password;
    WebSocket m_socket;
    GremlinClientResponse[UUID] m_responses;

    this(URL url, string user, string password) {
        m_url = url;
        m_user = user;
        m_password = password;

        connect;
    }

    this(string url, string user, string password) {
        this(URL(url), user, password);
    }

    void authenticate() {
        Request request;
        request.requestId = randomUUID;
        request.op = "authentication";
        request.args = Json.emptyObject;
        request.args["SASL"] = Json("\0" ~ m_user ~ "\0" ~ m_password);

        send(request);
    }

    void close() {
        m_socket.close();
    }

    void connect() {
        m_socket = connectWebSocket(m_url);

        runTask(&handleIncoming);
    }

    void handleIncoming() {
        while (m_socket.waitForData) {
            auto response = m_socket
                .receiveBinary
                .assumeUTF
                .parseJsonString
                .deserializeJson!Response;

            if (response.status.code == HTTPStatus.proxyAuthenticationRequired) {
                authenticate;
                continue;
            }

            m_responses[response.requestId].data ~= response.result;

            if (response.status.code == HTTPStatus.PartialContent) {
                continue;
            }

            m_responses[response.requestId].status = response.status;
        }
    }

    @trusted
    auto query(string queryString) {
        Request request;
        request.requestId = randomUUID;
        request.op = "eval";
        request.args = Json.emptyObject;
        request.args["gremlin"] = Json(queryString);

        m_responses[request.requestId] = new GremlinClientResponse;

        send(request);

        return async({
            while (m_responses[request.requestId].status.code == HTTPStatus.init && m_socket.connected) {
                yield;
            }

            return m_responses[request.requestId];
        });
    }

    void send(scope Request request) {
        m_socket.send(0x10 ~ "application/json" ~ request.serializeToJsonString);
    }
}

@safe
private struct Request {
    UUID requestId;
    string op;
    string processor;
    Json args;
}

@safe
private struct Response {
    UUID requestId;
    Status status;
    Json result;
    
    struct Status {
        HTTPStatus code;
        string message;
    }
}