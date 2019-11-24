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

void main() {
	auto client = GremlinClient(
		URL("wss://jochem-graph.gremlin.cosmos.azure.com:443/"),
		"/dbs/cloudclass/colls/core",
		"5TY7et7NKb0qxfkW2LJ7i4nk8pbxlfZxARUNHcD3bxer5ROyauNpiHxisXfErQWEWv2tLTAtHVHx4A6K2Jh3XA=="
	);

	temp(client);
	temp(client);
	temp(client);
}

import vibe.http.client;

class S {
	int id = 0;
	string a;
}

void temp(GremlinClient client) {
	auto s = new S();
	s.a = "lol";

	auto start = Clock.currTime;
	Future!(GremlinClientResponse)[] tasks;

	

	foreach (i; 0 .. 150) {
		/*auto fut = async(function(string a) {

			import vibe.core.core;
			sleep(1000.msecs);

			return new GremlinClientResponse();
		}, s.a);*/
		auto fut = client.queryAsync(`g.V().hasLabel("user")`);
		tasks ~= fut;
	}

	writeln(Clock.currTime - start);

	start = Clock.currTime;
	GremlinClientResponse[] results;
	
	foreach (task; tasks) {
		results ~= task.getResult();
	}
	writeln(Clock.currTime - start);
	//writeln(results[150].result);
}