
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
		driver :: bus_driver_type(),
		children :: dict(),
		listeners :: set(),
		stats_msg_out :: integer()
	} ).


-type node_action() :: fun( ( #state{} ) -> { any(), #state{} } ).
-type find_return_type() :: { ok, pid(), any() } | { notfound } | { error, any() }.



%% ------------------------------------------------------------------
%% API Function Exports
%% ------------------------------------------------------------------

-export([start_link/0]).

-export([start_node/4, start_node/5, set_running/2]).
-export([get_name/1, match_topic/2]).
-export([observe/5, forget/5, post/3, post/4, distribute/5]).


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



%%%%%%%%%% public get_name/1 %%%%%%%%%%
-spec get_name( pid() ) -> string().

get_name(Node) ->
	gen_server:call(Node, { call_get_name }).



%%%%%%%%%% public match_topic/2 %%%%%%%%%%
-spec match_topic( pid(), #topic{} ) -> boolean().

match_topic(Node, Topic) ->
	gen_server:call(Node, { call_match_topic, Topic }).



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



%%%%%%%%%% public post/3, post/4 %%%%%%%%%%
-spec post( pid(), #topic{}, any(), proplists:proplist() ) -> return_type().

post(Node, Topic, Mesg) -> post(Node, Topic, Mesg, []).

post(Node, Topic, Mesg, Options) ->
	case match_topic(Node, Topic) of
		true	-> gen_server:cast( Node, { node_post, Topic, Mesg, Options } )
	;	_		-> { error, "Invalid topic for this node" }
	end.



%%%%%%%%%% public distribute/4 %%%%%%%%%%
-spec distribute( pid(), integer(), list( string() ), any(), proplists:proplist() ) -> return_type().

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
		{ string:join( tl(NewState#state.name), "/" ), Result, Children },
		NewState };



%%%%%%%%%% handle call_get_name %%%%%%%%%%

handle_call( { call_get_name }, _From, State) ->
	{ reply, string:join( tl(State#state.name), "/" ), State };
	

	
%%%%%%%%%% handle call_match_topic %%%%%%%%%%

handle_call( { call_match_topic, Topic }, _From, State) ->
	{ reply, true, State };  %% todo



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
			Topic = bus_topic:create_from_list(FullParts),
			gen_server:cast( self(), { node_post, Topic, Mesg, Options } ),
			{ noreply, State }

			
	;	[H|T] ->
			case State#state.wildcard of
				true  -> throw( { error, "Inconsistent trie (wildcard nodes cannot have children)" } )
			;	_     -> ok
			end,
			forward_message(State, Secret, H, T, FullParts, Mesg, Options),
			forward_message(State, Secret, "+", T, FullParts, Mesg, Options),
			forward_message(State, Secret, "#", [], FullParts, Mesg, Options),
			{ noreply, State }
	end;



%%%%%%%%%% handle node_post %%%%%%%%%%

handle_cast( { node_post, Topic, Mesg, Options }, State) ->
	%% stuff with Options
	ListenTopic = bus_topic:create_from_list(tl(State#state.name)),
	Count = sets:fold( fun(Target, Acc) ->
					deliver_message( Target, Topic, Mesg, ListenTopic ),
					Acc + 1
				end,
				0, State#state.listeners )
			+ State#state.stats_msg_out,
	{ noreply, State#state{ stats_msg_out = Count } };



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


%%%%%%%%%% forall_then %%%%%%%%%%
-spec forall_then( pid(), integer(), node_action() ) -> any().

forall_then(Node, Secret, Action) ->
	gen_server:call(Node, { call_forall_then, Secret, Action }).



%%%%%%%%%% getmake_then %%%%%%%%%%
-spec getmake_then( pid(), integer(), list( string() ), node_action(), node_action() ) -> find_return_type().

getmake_then(Node, Secret, Parts, FoundAction, CreateAction) ->
	Token = random:uniform( 1 bsl 32 ),
	gen_server:cast( Node, { getmake_then, self(), Secret, Parts, Token, FoundAction, CreateAction } ),
	receive
		{ getmake_reply, Token, Result }  -> Result

	end.



%%%%%%%%%% findnode_then %%%%%%%%%%
-spec findnode_then( pid(), integer(), list( string() ), node_action() ) -> find_return_type().

findnode_then(Node, Secret, Parts, FoundAction) ->
	Token = random:uniform( 1 bsl 32 ),
	gen_server:cast( Node, { findnode_then, self(), Secret, Parts, Token, FoundAction } ),
	receive
		{ findnode_reply, Token, Result }  -> Result

	end.



%%%%%%%%%% forward_message %%%%%%%%%%

forward_message(State, Secret, X, Parts, FullParts, Mesg, Options) ->
	case dict:find(X, State#state.children) of
		{ ok, [Node] }  -> gen_server:cast( Node, { distribute, Secret, Parts, FullParts, Mesg, Options } )
	;	_               -> ok
	end.
    


%%%%%%%%%% deliver_message %%%%%%%%%%
-spec deliver_message( pid(), #topic{}, any(), valid_topic_type() ) -> ok.

deliver_message(Target, Topic, Mesg, Listen) ->
	erlang:display( {"Endpoint", Target, Topic, Mesg, Listen} ),
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
	{ ok, RootPid } = start_node( [[]], Secret, false, mode_startup ),
	CatchPid = spawn(?SERVER, eunit_catch, []),
	?debugMsg("create new test tree"),
	{ RootPid, Secret, CatchPid }.
	

node_test_() ->
	{setup,
	 fun() ->
		{ RootPid, Secret, CatchPid } = create_tst_tree(),
		Topic = "system/+/input",
		ListenTopic = bus_topic:create(Topic),
		{ ok, NodePid, _ } = getmake_then(RootPid, Secret, ListenTopic#wildcard_topic.parts, fun noop_action/1, fun noop_action/1),
		{ RootPid, Secret, CatchPid, Topic, NodePid }
	 end,
	 fun( { RootPid, Secret, CatchPid, Topic, NodePid } ) ->
		[?_assert( get_name(NodePid) =:= Topic ),
		 ?_assert( match_topic(NodePid, bus_topic:create("system/eunit/input") ) ),
		
		 ?_assert( ( CatchPid ! terminate ) =:= terminate )
		]
	 end
	}.
	
	
observe_node_test_() ->
	{setup,
	 fun () -> create_tst_tree() end,
	 fun( { RootPid, Secret, CatchPid } ) ->
		[?_assert( observe(RootPid, Secret, ["test", "chan"], CatchPid, "Sent as hello #1") ),
		 ?_assert( observe(RootPid, Secret, ["test"], CatchPid, "Sent as hello #2") ),
		 ?_assertNot( observe(RootPid, Secret, ["test", "chan"], CatchPid, "Sent as hello #3") ),
		 %?_assertThrow( {error, _S}, observe(RootPid, Secret + 1, ["test"], CatchPid, "Hello World") ),
		 
		 ?_assert( ( CatchPid ! terminate ) =:= terminate )
		]
	 end
	}.

