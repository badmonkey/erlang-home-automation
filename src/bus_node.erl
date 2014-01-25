
-module(bus_node).

-behaviour(gen_server).
-define(SERVER, ?MODULE).

-include("bus_topic.hrl").


-record(state,
    {
        secret :: integer(),
        children :: dict(),
        listeners :: set()
    } ).
  
%% ------------------------------------------------------------------
%% API Function Exports
%% ------------------------------------------------------------------

-export([start_link/0]).

-export([observe/5, forget/5]).


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


    
-spec observe( pid(), integer(), list( string() ), pid(), any() ) -> boolean() | { error, string() }.

observe(Node, Secret, Parts, AddWho, Hello) ->
    gen_server:cast( Node, { observe, self(), Secret, Parts, AddWho, Hello } ),
    receive
        { observe_reply, AddedWho, Added }  ->
            erlang:display( {"Reply", AddedWho, Added} ),
            case AddedWho =/= AddWho of
                true  -> { error, "mismatch pids" };
                _     -> Added
            end
    end.


    
forget(Node, Secret, Parts, ForgetWho, Goodbye) ->
    gen_server:cast( Node, { forget, self(), Secret, Parts, ForgetWho, Goodbye } ).

    
%distribute(Node, Secret, Topic, Mesg) ->


%get_target(Secret, Topic) -> fun()


%% ------------------------------------------------------------------
%% gen_server Function Definitions
%% ------------------------------------------------------------------

init( { Secret } ) ->
    { ok,
      #state{
          secret = Secret,
          children = dict:new(),
          listeners = sets:new()
		} }.

    
    
handle_call(_Request, _From, State) ->
    {reply, ok, State}.



handle_cast( { observe, ReplyWho, Secret, Parts, AddWho, Hello }, State ) ->
    erlang:display( {"Observe", ReplyWho, Secret, Parts, AddWho, Hello } ),
    
    case Secret =/= State#state.secret of
        true  -> throw( {error, "Inconsistent trie (passed wrong secret)"} );
        _     -> ok
    end,
    
    case Parts of
        []    ->
            case sets:is_element(AddWho, State#state.listeners) of
                true  -> 
                    ReplyWho ! { observe_reply, AddWho, false },
                    { noreply, State };
                         
                _     ->
                    ReplyWho ! { observe_reply, AddWho, true },
                    % send Hello to AddWho
                    { noreply,
                      #state{
                          secret    = State#state.secret,
                          children  = State#state.children,
                          listeners = sets:add_element(AddWho, State#state.listeners)
                      }
                    }
            end;
            
        [H|T] ->
            case dict:find(H, State#state.children) of
                { ok, [Node] }  ->
                    erlang:display( {"Alive", Node} ),
                    gen_server:cast( Node, { observe, ReplyWho, Secret, T, AddWho, Hello } ),
                    erlang:display( {"Pass", Node, T} ),
                    { noreply, State };
                          
                _             ->
                    erlang:display( {"NewNode", H} ),
                    case gen_server:start_link(bus_node, {State#state.secret}, []) of
                        { ok, NewNode } ->  
                            erlang:display( {"Pass", NewNode, T} ),
                            gen_server:cast( NewNode, { observe, ReplyWho, Secret, T, AddWho, Hello } ),
                            { noreply,
                              #state{
                                  secret    = State#state.secret,
                                  listeners = State#state.listeners,
                                  children  = dict:append(H, NewNode, State#state.children)
                              }
                            };
                            
                        Other       -> 
                            erlang:display( {"Badness", Other} ),
                            { error, Other }
                    end
            end
    end;
    
    
handle_cast( { forget, ReplyWho, Secret, Parts, ForgetWho, Goodbye }, State ) ->
    erlang:display( {"Forget", ReplyWho, Secret, Parts, ForgetWho, Goodbye } ),
    
    case Secret =/= State#state.secret of
        true  -> throw( {error, "Inconsistent trie (passed wrong secret)"} );
        _     -> ok
    end,
    {noreply, State};
    
    
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

