-module(distributed_native).

-type 'ClientInfo'() :: node().

-type 'CheckinResponse'() :: #{}.

-type 'SetupConfig'() ::
      #{label                   => module(),
        data                    => term()
       }.

-type 'SetupResponse'() ::
      #{success                 => boolean(),
        data                    => term()
       }.

-type 'CleanupInfo'() :: #{final => boolean()}.

-type 'CleanupResponse'() :: #{}.

-type 'ShutdownRequest'() ::
      #{force                   => boolean() | 0 | 1 % = 1
       }.

-type 'ShutdownAck'() ::
      #{
       }.

-export_type([
	'ClientInfo'/0, 
	'CheckinResponse'/0, 
	'SetupConfig'/0, 
	'SetupResponse'/0, 
	'CleanupInfo'/0, 
	'CleanupResponse'/0,
	'ShutdownRequest'/0,
	'ShutdownAck'/0]).

-export([
	new_client_info/1,
	new_checkin_reponse/0,
	new_checkin_reponse_msg/0,
	new_setup_config/2,
	new_setup_response/2,
	new_setup_response_msg/2,
	new_cleanup_info/1,
	new_cleanup_response/0,
	new_cleanup_response_msg/0,
	new_shutdown_request/1,
	new_shutdown_ack/0,
	new_shutdown_ack_msg/0
	]).

%%%% Type Constructors %%%%

-spec new_client_info(Node :: node()) -> 'ClientInfo'().
new_client_info(Node) ->
	Node.

-spec new_checkin_reponse() -> 'CheckinResponse'().
new_checkin_reponse() ->
	#{}.

-spec new_checkin_reponse_msg() -> {'CheckinResponse', 'CheckinResponse'()}.
new_checkin_reponse_msg() ->
	{'CheckinResponse', new_checkin_reponse()}.

-spec new_setup_config(Module :: module(), Data :: term()) -> 'SetupConfig'().
new_setup_config(Module, Data) ->
	#{label => Module, data => Data}.

-spec new_setup_response(Success :: boolean(), Data :: term()) -> 'SetupResponse'().
new_setup_response(Success, Data) ->
	#{success => Success, data => Data}.

-spec new_setup_response_msg(Success :: boolean(), Data :: term()) -> {'SetupResponse', 'SetupResponse'()}.
new_setup_response_msg(Success, Data) ->
	{'SetupResponse', new_setup_response(Success, Data)}.

-spec new_cleanup_info(Final :: boolean()) -> 'CleanupInfo'().
new_cleanup_info(Final) ->
	#{final => Final}.

-spec new_cleanup_response() -> 'CleanupResponse'().
new_cleanup_response() ->
	#{}.

-spec new_cleanup_response_msg() -> {'CleanupResponse', 'CleanupResponse'()}.
new_cleanup_response_msg() ->
	{'CleanupResponse', new_cleanup_response()}.

-spec new_shutdown_request(Force :: boolean()) -> 'ShutdownRequest'().
new_shutdown_request(Force) ->
	#{force => Force}.

-spec new_shutdown_ack() -> 'ShutdownAck'().
new_shutdown_ack() ->
	#{}.

-spec new_shutdown_ack_msg() -> {'ShutdownAck', 'ShutdownAck'()}.
new_shutdown_ack_msg() ->
	{'ShutdownAck', new_shutdown_ack()}.
