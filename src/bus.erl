
-module(bus).
-behaviour(gen_server).

-define(SERVER, ?MODULE).

-include("bus_topic.hrl").


-record(state,
	{
		nodes :: dict()
	} ).


%% ------------------------------------------------------------------
%% API Function Exports
%% ------------------------------------------------------------------

-export([start_link/0]).

-export([subscribe/1, unsubscribe/1, publish/2, publish/3]).



%% ------------------------------------------------------------------
%% gen_server Function Exports
%% ------------------------------------------------------------------

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

%% ------------------------------------------------------------------
%% API Function Definitions
%% ------------------------------------------------------------------

start_link() ->
	gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).


	
-spec subscribe( string() ) -> ok | { error, string() }.

subscribe(TopicStr) ->
	Topic = bus_topic:create(TopicStr),
	case Topic of
		#bad_topic{ reason = Reason }	-> { error, Reason };
		_								-> gen_server:call(?SERVER, {subscribe, Topic})
	end.



-spec unsubscribe( string() ) -> ok | { error, string() }.

unsubscribe(TopicStr) ->
	Topic = bus_topic:create(TopicStr),
	case Topic of
		#bad_topic{ reason = Reason }	-> { error, Reason };
		_								-> gen_server:call(?SERVER, {unsubscribe, Topic})
	end.



-spec publish( string() | #topic{}, any(), list() ) -> ok | { error, string() }.

publish(Topic, Mesg) -> publish(Topic, Mesg, []).

publish( #topic{} = Topic, Mesg, Options ) ->
	gen_server:call(?SERVER, {publish, Topic, Mesg, Options});
	
publish(TopicStr, Mesg, Options) ->
	Topic = bus_topic:create(TopicStr),
	case Topic of
		#topic{}	-> publish(Topic, Mesg, Options);
		_			-> { error, "Invalid topic" }
	end.


%% ------------------------------------------------------------------
%% gen_server Function Definitions
%% ------------------------------------------------------------------

init(_Args) ->
	{ ok, #state{
			nodes = dict:new()
		} }.



handle_call( {subscribe, #topic{ parts = Topic } }, _From, State ) ->
	erlang:display( { "Topic", Topic, _From, State } ),
	{ reply, ok, State };

handle_call( {subscribe, #wildcard_topic{ parts = Topic } }, _From, State ) ->
	erlang:display( { "Wildcard", Topic, _From, State } ),
	{ reply, ok, State };
	
handle_call(_Request, _From, State) ->
	erlang:display( { "Unknown", _Request, _From, State } ),
	{ reply, ok, State }.


handle_cast(_Msg, State) ->
	{noreply, State}.


handle_info(_Info, State) ->
	{noreply, State}.


terminate(_Reason, _State) ->
	ok.

code_change(_OldVsn, State, _Extra) ->
	{ok, State}.

%% ------------------------------------------------------------------
%% Internal Function Definitions
%% ------------------------------------------------------------------

