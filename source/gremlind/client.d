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

import vibe.core.log;

static GremlinConnection[string][string] m_connections;

@trusted
auto getConnection(string url, string user, string password) {
    if (url !in m_connections || user !in m_connections[url]) {
        m_connections[url][user] = new GremlinConnection(url, user, password);
    }

    return m_connections[url][user];
}

@trusted
auto queryAsync(GremlinClient client, string queryString) {
    return async(function(immutable(string) url, immutable(string) user, immutable(string) password, immutable(string) queryString) {
        return getConnection(url, user, password).query(queryString);
    }, client.m_url.to!string, client.m_user, client.m_password, queryString);
}

@safe
struct GremlinClient {
    this(URL url, string user, string password) {
        m_url = url;
        m_user = user;
        m_password = password;
    }

    this(string url, string user, string password) {
        this(URL(url), user, password);
    }

    auto query(string queryString) {
        auto connection = getConnection(m_url.to!string, m_user, m_password);

        return connection.query(queryString);
    }

    private {
        URL m_url;
        string m_user;
        string m_password;
    }
}

@safe
class GremlinConnection {
    private {
        URL m_url;
        string m_user;
        string m_password;
        WebSocket m_socket;
        Request[UUID] m_requests;
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
            auto request = new Request;
            request.requestId = randomUUID;
            request.op = "authentication";
            request.args["SASL"] = Json("\0" ~ m_user ~ "\0" ~ m_password);

            send(request);
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
}

@safe
class GremlinClientResponse {
    Response.Status status;
    Json result;

    this() {

    }
    
    public {
        this(Request request) {
            this.status = request.response.status;
            this.result = request.response.result;
        }
    }
}

@safe
public {
    class Request {
        UUID requestId;
        string op;
        string processor;
        Json args;

        @ignore
        Response response;

        this() {
            args = Json.emptyObject;
            response = new Response();
        }
    }

    class Response {
        UUID requestId;
        Status status;
        Json result;
        
        struct Status {
            HTTPStatus code;
            string message;
        }
    }
}