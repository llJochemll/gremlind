import std.stdio;

import gremlind.client;
import vibe.inet.url;
import std.stdio;
import vibe.core.concurrency;
import std.conv;
import vibe.data.json;
import std.datetime;
import std.algorithm;
import std.array;
import vibe.core.core;

void main() {
	auto client = new GremlinClient(
		URL("wss://jochem-graph.gremlin.cosmos.azure.com:443/"),
		"/dbs/cloudclass/colls/core",
		"5TY7et7NKb0qxfkW2LJ7i4nk8pbxlfZxARUNHcD3bxer5ROyauNpiHxisXfErQWEWv2tLTAtHVHx4A6K2Jh3XA=="
	);

	temp(client);
	temp(client);
}

void temp(GremlinClient client) {
	auto start = Clock.currTime;
	Future!(GremlinClientResponse)[] tasks;

	foreach (i; 0 .. 10) {
		tasks ~= client.queryAsync(`g.V().hasLabel("user")`);
	}

	writeln(Clock.currTime - start);

	start = Clock.currTime;
	GremlinClientResponse[] results;
	
	foreach (task; tasks) {
		results ~= task.getResult();
	}
	writeln(Clock.currTime - start);
}