
-module(bus_node).

-behaviour(gen_server).
-define(SERVER, ?MODULE).

-include("bus.hrl").


-record(state,
	{
		name :: list(string()),
		secret :: integer(),
		run_mode :: bus_run_modes(),
		wildcard :: boolean(),
		children :: dict(),
		listeners :: set()
	} ).


%% ------------------------------------------------------------------
%% API Function Exports
%% ------------------------------------------------------------------

-export([start_link/0]).

-export([start_node/4, find_node/3, observe/5, forget/5, distribute/5]).


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



%%%%% public create_node/4 %%%%%
-spec start_node( list( string() ), integer(), boolean(), bus_run_modes() ) -> { ok, pid() } | { error, any() }.

start_node(Name, Secret, Wildcard, Mode) ->
	gen_server:start_link(bus_node, { Name, Secret, Wildcard, Mode }, []).



%%%%% public find_node_then/4 %%%%%
-spec find_node_then( pid(), integer(), list( string() ), node_action() ) -> none().

find_node_then(Node, Secret, Parts, Action) ->
	gen_server:cast( Node, { find_node_then, self(), Secret, Parts, Action } ),
	receive
		{ find_reply, ReplyNode }  -> ReplyNode

	end.
	


%%%%% public find_node/3 %%%%%
-spec find_node( pid(), integer(), list( string() ) ) -> pid().

find_node(Node, Secret, Parts) ->
	gen_server:cast( Node, { find_node, self(), Secret, Parts } ),
	receive
		{ find_reply, ReplyNode }  -> ReplyNode

	end.



%%%%% public observe/5 %%%%%
-spec observe( pid(), integer(), list( string() ), pid(), any() ) -> boolean() | { error, string() }.

observe(Node, Secret, Parts, AddWho, Hello) ->
	gen_server:cast( Node, { observe, self(), Secret, Parts, AddWho, Hello } ),
	receive
		{ observe_reply, AddedWho, Added }  ->
			case AddedWho =/= AddWho of
				true  -> { error, "mismatch: pid returned is not the pid requested" }
			;	_     -> erlang:display( {"Added", AddedWho, Added} ), Added
			end
	end.
	
	find_node_then(Node, Secret, Parts, 



%%%%% public forget/5 %%%%%
-spec forget( pid(), integer(), list( string() ), pid(), any() ) -> boolean() | { error, string() }.

forget(Node, Secret, Parts, ForgetWho, Goodbye) ->
	gen_server:cast( Node, { forget, self(), Secret, Parts, ForgetWho, Goodbye } ),
	receive
		{ forget_reply, ForgottenWho, Removed }  ->
			case ForgottenWho =/= ForgetWho of
				true  -> { error, "mismatch: pid returned is not the pid requested" }
			;	_     -> erlang:display( {"Forgotten", ForgottenWho, Removed} ), Removed
			end
	end.

    

%%%%% public distribute/4 %%%%%

distribute(Node, Secret, Parts, Mesg, Options) ->
	gen_server:cast( Node, { distribute, Secret, Parts, Parts, Mesg, Options } ).


%get_target(Secret, Topic) -> fun()


%% ------------------------------------------------------------------
%% gen_server Function Definitions
%% ------------------------------------------------------------------

init( { Name, Secret, Wildcard, Mode } ) ->
	erlang:display( {"NewNode", Name, self()} ),
	{ ok,
		#state{
			name = Name,
			secret = Secret,
			run_mode = Mode,
			wildcard = Wildcard,
			children = dict:new(),
			listeners = sets:new()
		}
	}.

    
    
handle_call(_Request, _From, State) ->
	erlang:display( {"Ignoring call", _Request, _From} ),
	{reply, ok, State}.



%%%%% handle observe %%%%%

handle_cast( { observe, ReplyWho, Secret, Parts, AddWho, Hello }, State ) ->

	case Secret =/= State#state.secret of
		true  -> throw( {error, "Inconsistent trie (passed wrong secret)"} )
	;	_     -> ok
	end,

	case Parts of
		[]    ->
			case sets:is_element(AddWho, State#state.listeners) of
				true  -> 
					ReplyWho ! { observe_reply, AddWho, false },
					{ noreply, State }
							
			;	_     ->
					ReplyWho ! { observe_reply, AddWho, true },
					deliver_message( AddWho, bus:topic_private(AddWho), Hello, bus:topic_everything() ),
					{ noreply,
						State#state{
							listeners = sets:add_element(AddWho, State#state.listeners)
						}
					}
			end;
			
		[H|T] ->
			case State#state.wildcard of
				true  -> throw( {error, "Inconsistent trie (wildcard nodes cannot have children)"} )
			;	_     -> ok
			end,
			
			case dict:find(H, State#state.children) of
				{ ok, [Node] }  ->
					gen_server:cast( Node, { observe, ReplyWho, Secret, T, AddWho, Hello } ),
					{ noreply, State }
							
			;	_             ->
					case start_node(State#state.name ++ [H], State#state.secret, H =:= "#", State#state.run_mode) of
						{ ok, NewNode } ->  
							gen_server:cast( NewNode, { observe, ReplyWho, Secret, T, AddWho, Hello } ),
							{ noreply,
								State#state{
									children  = dict:append(H, NewNode, State#state.children)
								}
							}
							
					;	Other       -> 
							{ error, Other }
					end
			end
	end;
    
    

%%%%% handle forget %%%%%

handle_cast( { forget, ReplyWho, Secret, Parts, ForgetWho, Goodbye }, State ) ->

	case Secret =/= State#state.secret of
		true  -> throw( {error, "Inconsistent trie (passed wrong secret)"} );
		_     -> ok
	end,

	case Parts of
		[]    ->
			case sets:is_element(ForgetWho, State#state.listeners) of
				true  -> 
					ReplyWho ! { forget_reply, ForgetWho, true },
					deliver_message( ForgetWho, bus:topic_private(ForgetWho), Goodbye, bus:topic_everything() ),
					{ noreply,
						State#state{
							listeners = sets:del_element(ForgetWho, State#state.listeners)
						}
					}
							
			;	_     ->
					ReplyWho ! { forget_reply, ForgetWho, false },
					{ noreply, State }
			end;
			
		[H|T] ->
			case dict:find(H, State#state.children) of
				{ ok, [Node] }  ->
					gen_server:cast( Node, { forget, ReplyWho, Secret, T, ForgetWho, Goodbye } ),
					{ noreply, State }
							
			;	_             ->
					ReplyWho ! { forget_reply, ForgetWho, { error, "Incomplete or bad topic specified" } },
					{ noreply, State }
			end        
	end;



%%%%% handle distribute %%%%%

handle_cast( { distribute, Secret, Parts, FullParts, Mesg, Options }, State) ->

	case Secret =/= State#state.secret of
		true  -> throw( {error, "Inconsistent trie (passed wrong secret)"} )
	;	_     -> ok
	end,
	erlang:display( {"Visiting", State#state.name, self(), Mesg} ),
	case Parts of
		[]    ->
			% process options
			Topic = bus_topic:create_from_list(FullParts),
			ListenTopic = bus_topic:create_from_list(tl(State#state.name)),
			deliver_message( self(), Topic, Mesg, ListenTopic ),
			{ noreply, State }

			
	;	[H|T] ->
			case State#state.wildcard of
				true  -> throw( { error, "Inconsistent trie (wildcard nodes cannot have children)" } )
			;	_     -> ok
			end,
			spread_message(State, Secret, H, T, FullParts, Mesg, Options),
			spread_message(State, Secret, "+", T, FullParts, Mesg, Options),
			spread_message(State, Secret, "#", [], FullParts, Mesg, Options),
			{ noreply, State }
	end;



handle_cast(_Msg, State) ->
	erlang:display( {"Ignoring cast", _Msg} ),
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


spread_message(State, Secret, X, Parts, FullParts, Mesg, Options) ->
	case dict:find(X, State#state.children) of
		{ ok, [Node] }  -> gen_server:cast( Node, { distribute, Secret, Parts, FullParts, Mesg, Options } )
	;	_               -> ok
	end.
    


-spec( deliver_message( pid(), #topic{}, any(), valid_topic_type() ) -> ok ).

deliver_message(Pid, Topic, Mesg, Listen) ->
	erlang:display( {"Endpoint", Pid, Topic, Mesg, Listen} ),
	%Pid ! {},
	ok.


