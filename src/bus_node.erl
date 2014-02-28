
-module(bus_node).
-include_lib("eunit/include/eunit.hrl").

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


-type node_action() :: fun( ( #state{} ) -> { any(), #state{} } ).
-type node_result_type() :: { ok, pid(), any() } | { notfound } | { error, any() }.



%% ------------------------------------------------------------------
%% API Function Exports
%% ------------------------------------------------------------------

-export([start_link/0]).

-export([start_node/4, start_node/5, set_running/2]).
-export([get_name/1]).
-export([observe/5, forget/5, distribute/5]).


%% ------------------------------------------------------------------
%% gen_server Function Exports
%% ------------------------------------------------------------------

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
		terminate/2, code_change/3]).
-export([eunit_catch/0]).

         
%% ------------------------------------------------------------------
%% API Function Definitions
%% ------------------------------------------------------------------

start_link() ->
	gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).



%%%%%%%%%% start_node/4,5 %%%%%%%%%%
-spec start_node( list( string() ), integer(), boolean(), bus_run_modes(), node_action() ) -> { ok, pid() } | { error, any() }.

noop_action(S) -> { true, S }.

start_node(Name, Secret, Wildcard, Mode) ->
	start_node(Name, Secret, Wildcard, Mode, fun noop_action/1 ).
	
start_node(Name, Secret, Wildcard, Mode, CreateAction) ->
	gen_server:start_link(bus_node, { Name, Secret, Wildcard, Mode, CreateAction }, []).



%%%%%%%%%% set_running/2 %%%%%%%%%%
-spec set_running( pid(), integer() ) -> ok.

set_running(Node, Secret) ->
	forall_then(Node, Secret, fun(S) -> { ok, S#state{ run_mode = mode_running } } end ),
	ok.



%%%%%%%%%% forall_then/3 %%%%%%%%%%
-spec forall_then( pid(), integer(), node_action() ) -> any().

forall_then(Node, Secret, Action) ->
	gen_server:call(Node, { call_forall_then, Secret, Action }).



%%%%%%%%%% getmake_then/5 %%%%%%%%%%
-spec getmake_then( pid(), integer(), list( string() ), node_action(), node_action() ) -> node_result_type().

getmake_then(Node, Secret, Parts, FoundAction, CreateAction) ->
	Token = random:uniform( 1 bsl 32 ),
	gen_server:cast( Node, { getmake_then, self(), Secret, Parts, Token, FoundAction, CreateAction } ),
	receive
		{ getmake_reply, Token, Result }  -> Result

	end.



%%%%%%%%%% findnode_then/4 %%%%%%%%%%
-spec findnode_then( pid(), integer(), list( string() ), node_action() ) -> node_result_type().

findnode_then(Node, Secret, Parts, FoundAction) ->
	Token = random:uniform( 1 bsl 32 ),
	gen_server:cast( Node, { findnode_then, self(), Secret, Parts, Token, FoundAction } ),
	receive
		{ findnode_reply, Token, Result }  -> Result

	end.

	

%%%%%%%%%% public get_name/1 %%%%%%%%%%
-spec get_name( pid() ) -> string().

get_name(Node) ->
	gen_server:call(Node, { call_get_name }).
	


%%%%%%%%%% public observe/5 %%%%%%%%%%
-spec observe( pid(), integer(), list( string() ), pid(), any() ) -> boolean() | { error, string() }.

observe(Node, Secret, Parts, AddWho, Hello) ->
	case getmake_then(Node, Secret, Parts,
				fun(AState) ->
					add_observer(AddWho, AState)
				end,
				fun noop_action/1 ) of
		{ ok, _NodePid, DidAdd } 	->
			case DidAdd of
				true	-> deliver_message( AddWho, bus:topic_private(AddWho), Hello, bus:topic_everything() )
			;	_		-> ok
			end,
			DidAdd

	;	{ error, Mesg }				-> { error, Mesg }
	end.



%%%%%%%%%% public forget/5 %%%%%%%%%%
-spec forget( pid(), integer(), list( string() ), pid(), any() ) -> boolean() | { error, string() }.

forget(Node, Secret, Parts, ForgetWho, Goodbye) ->
	case findnode_then(Node, Secret, Parts,
				fun(AState) ->
					remove_observer(ForgetWho, AState)
				end) of
		{ ok, _NodePid, DidDel } 	->
			case DidDel of
				true	-> deliver_message( ForgetWho, bus:topic_private(ForgetWho), Goodbye, bus:topic_everything() )
			;	_		-> ok
			end,
			DidDel
			
	;	{ notfound }				-> false
	;	{ error, Mesg }				-> { error, Mesg }
	end.

    

%%%%%%%%%% public distribute/4 %%%%%%%%%%

distribute(Node, Secret, Parts, Mesg, Options) ->
	gen_server:cast( Node, { distribute, Secret, Parts, Parts, Mesg, Options } ).



%% ------------------------------------------------------------------
%% gen_server Function Definitions
%% ------------------------------------------------------------------

init( { Name, Secret, Wildcard, Mode, CreateAction } ) ->
	erlang:display( {"NewNode", Name, self()} ),
	{ _Ignore, NewState } = CreateAction(
		#state{
			name = Name,
			secret = Secret,
			run_mode = Mode,
			wildcard = Wildcard,
			children = dict:new(),
			listeners = sets:new()
		}),
	{ ok, NewState }.

	
	
%%%%%%%%%% handle call_forall_then %%%%%%%%%%

handle_call( { call_forall_then, Secret, Action }, _From, State) ->

	case Secret =/= State#state.secret of
		true	-> throw( { error, "Inconsistent trie (passed wrong secret)" } )
	;	_		-> ok
	end,
	
	{ Result, NewState } = Action(State),
	
	Children = dict:fold( fun(_K, V, Acc) ->
			Acc ++ [ gen_server:call(V, { call_forall_then, Secret, Action } ) ]
		end,
		[],
		NewState#state.children),
	
	{ reply,
		{ string:join( NewState#state.name, "/" ), Result, Children },
		NewState };



%%%%%%%%%% handle call_get_name %%%%%%%%%%

handle_call( { call_get_name }, _From, State) ->
	{ reply, string:join( State#state.name, "/" ), State };

    

%%%%%%%%%% handle default call %%%%%%%%%%

handle_call(_Request, _From, State) ->
	erlang:display( {"Ignoring call", _Request, _From} ),
	{ reply, ok, State }.

  
  
%%%%%%%%%% handle getmake_then %%%%%%%%%%

handle_cast( { getmake_then, ReplyWho, Secret, Parts, Token, FoundAction, CreateAction }, State ) ->

	case Secret =/= State#state.secret of
		true	-> throw( { error, "Inconsistent trie (passed wrong secret)" } )
	;	_		-> ok
	end,

	case Parts of
		[]    ->
			{ Result, NextState } = FoundAction(State),
			ReplyWho ! { getmake_reply, Token, { ok, self(), Result } },
			{ noreply, NextState }
			
	;	[H|T] ->
			case State#state.wildcard of
				true	-> throw( {error, "Inconsistent trie (wildcard nodes cannot have children)"} )
			;	_		-> ok
			end,
			
			case dict:find(H, State#state.children) of
				{ ok, [Node] }	->
					gen_server:cast( Node, { getmake_then, ReplyWho, Secret, T, Token, FoundAction, CreateAction } ),
					{ noreply, State }
							
			;	_				->
					case start_node(State#state.name ++ [H], State#state.secret, H =:= "#", State#state.run_mode, CreateAction) of
						{ ok, NewNode }	->  
							gen_server:cast( NewNode, { getmake_then, ReplyWho, Secret, T, Token, FoundAction, CreateAction } ),
							{ noreply,
								State#state{
									children  = dict:append(H, NewNode, State#state.children)
								}
							}
							
					;	Other			-> 
							ReplyWho ! { getmake_reply, Token, { error, Other } },
							{ noreply, State }
					end
			end
	end;
        


%%%%%%%%%% handle findnode_then %%%%%%%%%%

handle_cast( { findnode_then, ReplyWho, Secret, Parts, Token, FoundAction }, State ) ->

	case Secret =/= State#state.secret of
		true	-> throw( { error, "Inconsistent trie (passed wrong secret)" } )
	;	_		-> ok
	end,

	case Parts of
		[]		->
			ActionWho = self(),
			{ Result, NextState } = FoundAction( ActionWho, State ),
			ReplyWho ! { findnode_reply, Token, { ok, ActionWho, Result } },
			{ noreply, NextState }
			
	;	[H|T]	->
			case dict:find(H, State#state.children) of
				{ ok, [Node] }	->
					gen_server:cast( Node, { findnode_reply, ReplyWho, Secret, T, Token, FoundAction } ),
					{ noreply, State }
							
			;	_				->
					ReplyWho ! { findnode_reply, Token, { notfound } },
					{ noreply, State }
			end        
	end;
	


%%%%%%%%%% handle distribute %%%%%%%%%%

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
			_Count = sets:fold( fun(Target, Acc) ->
					deliver_message( Target, Topic, Mesg, ListenTopic ),
					Acc + 1
				end,
				0, State#state.listeners),
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



%%%%%%%%%% handle default cast %%%%%%%%%%

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

%%%%%%%%%% spread_message %%%%%%%%%%
spread_message(State, Secret, X, Parts, FullParts, Mesg, Options) ->
	case dict:find(X, State#state.children) of
		{ ok, [Node] }  -> gen_server:cast( Node, { distribute, Secret, Parts, FullParts, Mesg, Options } )
	;	_               -> ok
	end.
    


%%%%%%%%%% deliver_message %%%%%%%%%%
-spec deliver_message( pid(), #topic{}, any(), valid_topic_type() ) -> ok.

deliver_message(Pid, Topic, Mesg, Listen) ->
	erlang:display( {"Endpoint", Pid, Topic, Mesg, Listen} ),
	%Pid ! { bus_message, Mesg },
	ok.


	
%%%%%%%%%% add_observer %%%%%%%%%%

add_observer(AddWho, State) ->
	case sets:is_element(AddWho, State#state.listeners) of
		true  -> 
			{ false, State }
					
	;	_     ->
			{ true,
				State#state{
					listeners = sets:add_element(AddWho, State#state.listeners)
				}
			}
	end.
	    

		
%%%%%%%%%% remove_observer %%%%%%%%%%

remove_observer(ForgetWho, State) ->
	case sets:is_element(ForgetWho, State#state.listeners) of
		true  -> 
			{ true,
				State#state{
					listeners = sets:del_element(ForgetWho, State#state.listeners)
				}
			}
					
	;	_     ->
			{ false, State }
	end.



%% ------------------------------------------------------------------
%% EUnit Definitions
%% ------------------------------------------------------------------

eunit_catch() ->
	?debugMsg("euint_catch started"),
	receive
		terminate	-> ?debugMsg("Terminating"), ok
	;	_Ignore		-> ?debugMsg("Eating message"), eunit_catch()
	end.


create_tst_tree() ->
	random:seed( now() ),
	Secret = random:uniform( 1 bsl 32 ),
	{ ok, Pid } = start_node( [[]], Secret, false, mode_startup ),
	CatchPid = spawn(?SERVER, eunit_catch, []),
	?debugMsg("create new test tree"),
	{ Pid, Secret, CatchPid }.
	


basic_node_test_() ->
	{setup,
	 fun () -> create_tst_tree() end,
	 fun( { Pid, Secret, CatchPid } ) ->
		[?_assert( observe(Pid, Secret, ["test"], CatchPid, "Hello World") ),
		 ?_assertNot( observe(Pid, Secret, ["test"], CatchPid, "Hello World") ),
		 ?_assert( observe(Pid, Secret, ["test", "chan"], CatchPid, "Hello World") ),
		 %?_assertThrow( {error, _S}, observe(Pid, Secret + 1, ["test"], CatchPid, "Hello World") ),
		 
		 ?_assert( ( CatchPid ! terminate ) =:= terminate )
		]
	 end
	}.

	