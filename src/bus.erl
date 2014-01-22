
-module(bus).
-behaviour(gen_server).

-define(SERVER, ?MODULE).


-record(state, { name }).


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



subscribe(Topic) ->
    ok = gen_server:call(?SERVER, {subscribe, Topic}),
%    receive
%        {?MODULE, subscribe_reply, _} -> ok
%    end.
    ok.


unsubscribe(Topic) ->
    ok = gen_server:call(?SERVER, {unsubscribe, Topic}),
%    receive
%        {?MODULE, unsubscribe_reply, _} -> ok
%    end.
    ok.


publish(Topic, Mesg) -> publish(Topic, Mesg, []).

publish(Topic, Mesg, Options) ->
    ok = gen_server:call(?SERVER, {publish, Topic, Mesg, Options}).



%% ------------------------------------------------------------------
%% gen_server Function Definitions
%% ------------------------------------------------------------------

init(_Args) ->
    { ok, #state{ name = "Bob" } }.



handle_call(_Request, _From, State) ->
    erlang:display( { _Request, _From, State } ),
    {reply, ok, State}.

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

