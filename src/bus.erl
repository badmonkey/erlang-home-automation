
-module(bus).
-behaviour(gen_server).

-define(SERVER, ?MODULE).

-include("bus_topic.hrl").


-record(state,
    {
        secret :: integer(),
        noderoot :: pid()
    } ).


%% ------------------------------------------------------------------
%% API Function Exports
%% ------------------------------------------------------------------

-export([start_link/0]).

-export([subscribe/1, subscribe/2, unsubscribe/1, unsubscribe/2, publish/2, publish/3]).



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


	
-spec subscribe( string() | valid_topic_type(), proplists:proplist() ) -> ok | { error, string() }.
% Options
%   { send_hello, term() }

subscribe(Topic) -> subscribe(Topic, []).

subscribe( #topic{} = Topic, Options) ->
    gen_server:call(?SERVER, { subscribe, Topic, self(), Options });
    
subscribe( #wildcard_topic{} = Topic, Options) ->
    gen_server:call(?SERVER, { subscribe, Topic, self(), Options });

subscribe(TopicStr, Options) ->
    Topic = bus_topic:create(TopicStr),
    case Topic of
        #bad_topic{ reason = Reason }	-> { error, Reason };
        _   -> gen_server:call(?SERVER, { subscribe, Topic, self(), Options })
    end.
    

    
-spec unsubscribe( string() | valid_topic_type(), proplists:proplist() ) -> ok | { error, string() }.
% Options
%   { send_goodbye, term() }

unsubscribe(Topic) -> unsubscribe(Topic, []).

unsubscribe( #topic{} = Topic, Options) ->
    gen_server:call(?SERVER, { unsubscribe, Topic, self(), Options });
    
unsubscribe( #wildcard_topic{} = Topic, Options) ->
    gen_server:call(?SERVER, { unsubscribe, Topic, self(), Options });
    
unsubscribe(TopicStr, Options) ->
    Topic = bus_topic:create(TopicStr),
    case Topic of
        #bad_topic{ reason = Reason }	-> { error, Reason };
        _   -> gen_server:call(?SERVER, {unsubscribe, Topic, self(), Options})
    end.



-spec publish( string() | #topic{}, any(), proplists:proplist() ) -> ok | { error, string() }.
% Options
%   retain

publish(Topic, Mesg) -> publish(Topic, Mesg, []).

publish( #topic{} = Topic, Mesg, Options) ->
    gen_server:call(?SERVER, { publish, Topic, Mesg, Options });
    
publish( TopicStr, Mesg, Options ) ->
    Topic = bus_topic:create(TopicStr),
    case Topic of
        #bad_topic{ reason = Reason }	-> { error, Reason };
        _   -> gen_server:call(?SERVER, { publish, Topic, Mesg, Options })
    end.
	

%% ------------------------------------------------------------------
%% gen_server Function Definitions
%% ------------------------------------------------------------------

init(_Args) ->
    random:seed( now() ),
    Secret = random:uniform( 1 bsl 32 ),
    case gen_server:start_link(bus_node, { [[]], Secret, false }, []) of
        { ok, Pid } ->  { ok,
                          #state{ secret = Secret, noderoot = Pid }
                        };
        Other       -> Other
    end.



handle_call( { subscribe, #topic{ parts = Topic }, AddWho, Options }, _From, State ) ->
    erlang:display( { "Topic", Topic, AddWho, _From, State } ),
    bus_node:observe(State#state.noderoot, State#state.secret, Topic, AddWho, proplists:get_value("send_hello", Options) ),
    { reply, ok, State };

    
    
handle_call( { subscribe, #wildcard_topic{ parts = Topic }, AddWho, Options }, _From, State ) ->
    erlang:display( { "Wildcard", Topic, AddWho, _From, State } ),
    bus_node:observe( State#state.noderoot, State#state.secret, Topic, AddWho, proplists:get_value("send_hello", Options) ),
    { reply, ok, State };
    
    
    
handle_call( { unsubscribe, #topic{ parts = Topic }, RemoveWho, Options }, _From, State ) ->
    erlang:display( { "un- Topic", Topic, RemoveWho, _From, State } ),
    bus_node:forget(State#state.noderoot, State#state.secret, Topic, RemoveWho, proplists:get_value("send_goodbye", Options) ),
    { reply, ok, State };

    
    
handle_call( { unsubscribe, #wildcard_topic{ parts = Topic }, RemoveWho, Options }, _From, State ) ->
    erlang:display( { "un- Wildcard", Topic, RemoveWho, _From, State } ),
    bus_node:forget( State#state.noderoot, State#state.secret, Topic, RemoveWho, proplists:get_value("send_goodbye", Options) ),
    { reply, ok, State };    

    
    
handle_call( { publish, #topic{ parts = Topic }, Mesg, Options }, _From, State ) ->
    erlang:display( { "Publish", Topic, Mesg, _From, State } ),
    bus_node:distribute(State#state.noderoot, State#state.secret, Topic, Mesg),
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

