
-module(bus_topic).
-include_lib("eunit/include/eunit.hrl").
-include("bus.hrl").

-export([create/1, create_from_list/1, topic_from_wildcard/1, match/2, is_valid_name/1, to_string/1]).



%%%%%%%%%% public is_valid_name/1 %%%%%%%%%%
-spec is_valid_name( string() ) -> boolean().

is_valid_name(ID) ->
	case re:run(ID, "^[a-zA-Z0-9_ ,%$!@<>()=:;.?|\-]+$") of 
		{ match, _capture }	-> true
	;	_					-> false
	end.


    
%%%%%%%%%% public create/1 %%%%%%%%%%
-spec create( string() ) -> all_topics_type().

create( S ) ->
	SplitList = re:split(S,"[/]",[{return,list}]),

		% handle empty strings and leading '/'
	case SplitList of
		[[]]	-> #bad_topic{ reason = "empty string passed as topic" }
	;	[[]|T]	-> forward_topic_type( SplitList, implement_create(T) )
	;	_		-> implement_create(SplitList)
	end.

    
    
%%%%%%%%%% public create_from_list/1 %%%%%%%%%%
-spec create_from_list( list(string()) ) -> all_topics_type().

create_from_list( L ) -> 
	% handle empty strings and leading '/'
	case L of
		[]		-> #bad_topic{ reason = "empty list passed as topic" }
	;	[[]]	-> #bad_topic{ reason = "empty list passed as topic" }
	;	[[]|T]	-> forward_topic_type( L, implement_create(T) )
	;	_		-> implement_create(L)
	end.



%%%%%%%%%% public topic_from_wildcard/1 %%%%%%%%%%
-spec topic_from_wildcard( #wildcard_topic{} | list(string()) ) -> all_topics_type().

topic_from_wildcard( #wildcard_topic{} = Topic ) ->
	Topic.



implement_create( [] )			-> #topic{ parts = [] };
implement_create( [[]] )		-> #bad_topic{ reason = "topic is incomplete (ends with a '/')" };
implement_create( ["#"] )		-> #wildcard_topic{ parts = ["#"] };
implement_create( ["#"|_T] )	-> #bad_topic{ reason = "hash in middle of topic" };

implement_create( ["+"|T] = Parts ) -> 
	TailTopic = implement_create(T),
	case TailTopic of
		#topic{}			-> #wildcard_topic{ parts = Parts }
	;	#wildcard_topic{}	-> #wildcard_topic{ parts = Parts }
	;	_					-> TailTopic
	end;

implement_create( [[]|_T] ) -> #bad_topic{ reason = "empty part name (using '//' in topic)" };

implement_create( [H|T] = Parts ) ->
	case is_valid_name(H) of
		true	-> forward_topic_type(Parts, implement_create(T))
	;	_		-> #bad_topic{ reason = "invalid part name" }
	end.



% forwards the topic type but uses Parts instead of Topic.parts
-spec forward_topic_type( list( string() ), all_topics_type() ) -> all_topics_type().

forward_topic_type(Parts, Topic) ->
	case Topic of
		#topic{}			-> #topic{ parts = Parts }
	;	#wildcard_topic{}	-> #wildcard_topic{ parts = Parts }
	;	_					-> Topic
	end.

    

%%%%%%%%%% public match/2 %%%%%%%%%%
-spec match( valid_topic_type(), valid_topic_type() ) -> boolean() | undefined.

match( #topic{ parts = Parts }, #topic{ parts = Parts2 } ) ->
	implement_match( Parts, Parts2 );
match( #topic{ parts = Parts }, #wildcard_topic{ parts = Parts2 } ) ->
	implement_match( Parts, Parts2 );
    
  % right types but in unusual order ... swap em and continue
match( #wildcard_topic{ parts = Parts }, #topic{ parts = Parts2 } ) ->
	implement_match( Parts2, Parts );
    
  % #wildcards can't match #wildcards (also handle all other cases)
match( _, _ ) -> undefined.


-spec implement_match( list( string() ), list( string() ) ) -> boolean().
implement_match( [], [] )				-> true;
implement_match( _, ["#"] )				-> true;

implement_match( [_H|T1], ["+"|T2] )	-> implement_match( T1, T2 );

implement_match( [H|T1], [H|T2] )		-> implement_match( T1, T2 );

implement_match( _, _ )					-> false.



%%%%%%%%%% to_string/1 %%%%%%%%%%
-spec to_string( valid_topic_type() ) -> string().

to_string( #topic{ parts = Parts } ) ->
	string:join( Parts, "/" );

to_string( #wildcard_topic{ parts = Parts } ) ->
	string:join( Parts, "/" ).



%% ------------------------------------------------------------------
%% EUnit Definitions
%% ------------------------------------------------------------------


valid_names_test_() ->
	[?_assertNot( is_valid_name("") ),

	 ?_assert( is_valid_name("abc123") ),
	 ?_assert( is_valid_name("a b c") ),
	
	 ?_assert( is_valid_name(" ") ),
	
	 ?_assertNot( is_valid_name("abc'") ),
	
	 ?_assertNot( is_valid_name("+'") ),
	 ?_assertNot( is_valid_name("abc+'") ),
	 ?_assertNot( is_valid_name("ab+cd'") ),
	
	 ?_assertNot( is_valid_name("#'") ),
	 ?_assertNot( is_valid_name("ab#'") ),
	 ?_assertNot( is_valid_name("ab#cd'") )
	].

	
	
create_topics_test_() ->
	[?_assertMatch( #bad_topic{}, create("") ),
	 ?_assertMatch( #bad_topic{}, create("/") ),
	
	 ?_assertMatch( #topic{}, create("/a/b/c") ),
	 ?_assertMatch( #topic{}, create("a/b/c") ),

	 ?_assertMatch( #topic{}, create("a/b c") ),
	 ?_assertMatch( #bad_topic{}, create("a/b+c") ),
	 ?_assertMatch( #bad_topic{}, create("a/b#c") ),
	 ?_assertMatch( #bad_topic{}, create("a/b+") ),
	 ?_assertMatch( #bad_topic{}, create("a/b#") ),

	 ?_assertMatch( #topic{}, create("a/b!c") ),
	 ?_assertMatch( #bad_topic{}, create("a/b'c") ),

	 ?_assertMatch( #bad_topic{}, create("a//b/c") ),
	 ?_assertMatch( #bad_topic{}, create("a/b/c/") ),
	 ?_assertMatch( #bad_topic{}, create("a/b/c//") ),

	 ?_assertMatch( #wildcard_topic{}, create("a/+/c") ),
	 ?_assertMatch( #wildcard_topic{}, create("a/+/+") ),
	 ?_assertMatch( #wildcard_topic{}, create("a/b/#") ),
	 ?_assertMatch( #topic{}, create("a/?/c") ),
	 ?_assertMatch( #bad_topic{}, create("a/#/c") ),
	 ?_assertMatch( #wildcard_topic{}, create("#") ),
	 ?_assertMatch( #wildcard_topic{}, create("/#") ),
	 
	 %dialyser burfs (correctly) at both these tests
	 %?_assertMatch( #bad_topic{}, create(100) ),
	 %?_assertMatch( #bad_topic{}, create( create("/a/b/c") ) ),
	 
	 ?_assertMatch( #topic{}, create( lists:concat(["/process/", io_lib:print(self()), "/control"]) ) )
	].


	
create_from_list_test_() ->
	[?_assertMatch( #bad_topic{}, create_from_list([]) ),
	 ?_assertMatch( #bad_topic{}, create_from_list([[]]) ),
	
	 ?_assertMatch( #topic{}, create_from_list(["a", "b"]) ),
	 ?_assertMatch( #topic{}, create_from_list([[], "a", "b"]) ),
	  
	 ?_assertMatch( #wildcard_topic{}, create_from_list(["a", "#"]) ),
	 ?_assertMatch( #bad_topic{}, create_from_list(["a", "#", "b"]) )
	].



maketest_to_string(S) ->
	?_assertEqual( to_string( create(S) ), S ).

to_string_test_() ->
	[maketest_to_string("a/b/c"),
	 maketest_to_string("/a"),
	 maketest_to_string("a/+/c")
	].
	


maketest_match(S1, S2, Expected) ->
	?_assertEqual( match( create(S1), create(S2) ), Expected ).
	
match_test_() ->
	[maketest_match("a/b/c", "a/b/c", true),
	 maketest_match("a/b/c", "a/b/d", false),
	 maketest_match("a/b/c", "/a/b/c", false),

	 maketest_match("a/b/c", "a/+/c", true),
	 maketest_match("a/b/c", "a/b/+", true),
	 maketest_match("a/+/c", "a/b/c", true),
	 maketest_match("a/+/c", "a/+/+", undefined),
	 maketest_match("a/b/c", "a/+/+", true),
	 maketest_match("a/b/c/d", "a/+/+", false),

	 maketest_match("/a", "+/+", true),
	 maketest_match("/a", "/+", true),
	 maketest_match("/a", "+", false),

	 maketest_match("a", "#", true),
	 maketest_match("/a", "#", true),
	 maketest_match("a", "/#", false),
	 maketest_match("/a", "/#", true),

	 maketest_match("a/b/c", "a/b/#", true),
	 maketest_match("a/b", "a/b/#", true),
	 maketest_match("a/b/c", "a/#", true),
	 maketest_match("a/b/c", "a/+/#", true)
	].

        
