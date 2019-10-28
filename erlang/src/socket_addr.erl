-module(socket_addr).

-export([
	addr/1,
	port/1,
	parse/1, 
	to_string/1]).

-record(ip_socket_addr, {addr :: string(), port :: integer()}).

-opaque ip() :: #ip_socket_addr{}.

-export_type([ip/0]).

-spec parse(string()) -> empty | {invalid_port, string()} | {invalid_address, string()} | {ok, ip()}.
parse([]) ->
	empty;
parse(Str) -> 
	case string:split(Str,":") of
		[Addr, PortStr] ->
			case string:to_integer(PortStr) of
				{Port,[]} ->
					{ok, #ip_socket_addr{addr=Addr,port=Port}};
				S -> 
					{invalid_port, S}
			end;
		S ->
			{invalid_address, S}
	end.

-spec to_string(ip()) -> string().
to_string(#ip_socket_addr{addr=Addr,port=Port}) ->
	io_lib:format("#ip_socket_addr{addr=~s,port=~b}", [Addr, Port]).

-spec port(ip()) -> integer().
port(#ip_socket_addr{addr=_Addr,port=Port}) ->
	Port.

-spec addr(ip()) -> string().
addr(#ip_socket_addr{addr=Addr,port=_Port}) ->
	Addr.
