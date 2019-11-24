module gremlind.client;

import std.conv;
import std.string;
import std.uuid;
import vibe.core.concurrency : async, makeIsolated, assumeIsolated;
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
        return m_connection.query(queryString);
    }

    @trusted
    auto queryAsync(string queryString) {
        return async(&(this.query), queryString);
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
    Json result;

    this(Request request) {
        this.status = request.response.status;
        this.result = request.response.result;
    }
}

@safe
private class GremlinConnection {
    URL m_url;
    string m_user;
    string m_password;
    WebSocket m_socket;
    Request[UUID] m_requests;

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
        auto request = new Request;
        request.requestId = randomUUID;
        request.op = "authentication";
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

            if (response.requestId in m_requests) {
                m_requests[response.requestId].response = response;
            }
        }
    }

    @trusted
    auto query(string queryString) {
        auto request = new Request;
        request.requestId = randomUUID;
        request.op = "eval";
        request.args["gremlin"] = Json(queryString);

        m_requests[request.requestId] = request;

        send(request);

        while (request.response.status.code == HTTPStatus.init) {
            yield;
        }

        auto response = new GremlinClientResponse(request);
        
        m_requests.remove(request.requestId);

        return response;
    }

    void send(scope Request request) {
        m_socket.send(0x10 ~ "application/json" ~ request.serializeToJsonString);
    }
}

@safe
private class Request {
    UUID requestId;
    string op;
    string processor;
    Json args;

    @ignore
    Response response;

    this() {
        args = Json.emptyObject;
    }
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