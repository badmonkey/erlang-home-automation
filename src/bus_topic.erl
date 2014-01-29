
-module(bus_topic).
-include("bus_topic.hrl").

-export([create/1, create_from_list/1, match/2, is_valid_name/1, test/0]).


%%%%% public is_valid_name/1 %%%%%
-spec is_valid_name( string() ) -> boolean().

is_valid_name(ID) ->
    case re:run(ID, "^[a-zA-Z0-9_ ,%$!@<>()=:;.?|\-]+$") of 
        { match, _capture } -> true;
        _                   -> false
    end.


    
%%%%% public create/1 %%%%%
-spec create( string() ) -> all_topics_type().

create( S ) ->
    SplitList = re:split(S,"[/]",[{return,list}]),
    
      % handle empty strings and leading '/'
    case SplitList of
        [[]]    -> #bad_topic{ reason = "empty string passed as topic" };
        [[]|T]  -> forward_topic_type( SplitList, implement_create(T) );
        _       -> implement_create(SplitList)
    end.

    
    
%%%%% public create_from_list/1 %%%%%
-spec create_from_list( list(string()) ) -> all_topics_type().

create_from_list( L ) -> 
    % handle empty strings and leading '/'
    case L of
        []      -> #bad_topic{ reason = "empty list passed as topic" };
        [[]]    -> #bad_topic{ reason = "empty list passed as topic" };
        [[]|T]  -> forward_topic_type( L, implement_create(T) );
        _       -> implement_create(L)
    end.



implement_create( [] )        -> #topic{ parts = [] };
implement_create( [[]] )      -> #bad_topic{ reason = "topic is incomplete (ends with a '/')" };
implement_create( ["#"] )     -> #wildcard_topic{ parts = ["#"] };
implement_create( ["#"|_T] )  -> #bad_topic{ reason = "hash in middle of topic" };

implement_create( ["+"|T] = Parts ) -> 
    TailTopic = implement_create(T),
    case TailTopic of
        #topic{}              -> #wildcard_topic{ parts = Parts };
        #wildcard_topic{}     -> #wildcard_topic{ parts = Parts };
        _                     -> TailTopic
    end;

implement_create( [[]|_T] )   -> #bad_topic{ reason = "empty part name (using '//' in topic)" };

implement_create( [H|T] = Parts ) ->
    case is_valid_name(H) of
        true  -> forward_topic_type(Parts, implement_create(T));
        _     -> #bad_topic{ reason = "invalid part name" }
    end.



% forwards the topic type but uses Parts instead of Topic.parts
-spec forward_topic_type( list( string() ), all_topics_type() ) -> all_topics_type().

forward_topic_type(Parts, Topic) ->
    case Topic of
        #topic{}          -> #topic{ parts = Parts };
        #wildcard_topic{} -> #wildcard_topic{ parts = Parts };
        _                 -> Topic
    end.

    

%%%%% public match/2 %%%%%
-spec match( valid_topic_type(), valid_topic_type() ) -> boolean() | undefined.

match( #topic{ parts = Parts }, #topic{ parts = Parts2 } ) ->
    implement_match( Parts, Parts2 );
match( #topic{ parts = Parts }, #wildcard_topic{ parts = Parts2 } ) ->
    implement_match( Parts, Parts2 );
    
  % right types but in unusual order ... swap em and continue
match( #wildcard_topic{ parts = Parts2 }, #topic{ parts = Parts } ) ->
    implement_match( Parts, Parts2 );
    
  % #wildcards can't match #wildcards (also handle all other cases)
match( _, _ ) -> undefined.


-spec implement_match( list( string() ), list( string() ) ) -> boolean().
implement_match( [], [] )             -> true;
implement_match( _, ["#"] )           -> true;

implement_match( [_H|T1], ["+"|T2] )  -> implement_match( T1, T2 );

implement_match( [H|T1], [H|T2] )     -> implement_match( T1, T2 );

implement_match( _, _ )               -> false.



%%%%% Unit Tests %%%%%

test_valid_name(S, Expect) ->
    Valid = is_valid_name(S),
    case Valid of
        Expect  -> ok;
        _       -> erlang:display( { "Valid?", S, "expected", Expect, "got", Valid } )
    end.
    

test_create(S, Result) ->
    Topic = create(S),
%    erlang:display( { S, "creates", Topic } ),
    case Topic of
        { Result, _ } -> ok;
        _             -> erlang:display( { "Create", S, "expected", Result, "got", Topic } )
    end.
    
    
test_create_list(L, Result) ->
    Topic = create_from_list(L),
%    erlang:display( { L, "creates", Topic } ),
    case Topic of
        { Result, _ } -> ok;
        _             -> erlang:display( { "CreateList", L, "expected", Result, "got", Topic } )
    end.
    
    
test_match(S1, S2, ShouldMatch) ->
    Match = match( create(S1), create(S2) ),
    case Match of
        ShouldMatch -> ok;
        _           -> erlang:display( { "Match?", S1, S2, "expected", ShouldMatch, "got", Match } )
    end.
    

%%%%% public test/0 %%%%%

test() ->
    test_valid_name("", false),
    
    test_valid_name("abc123", true),
    test_valid_name("a b c", true),
    
    test_valid_name(" ", true),
    
    test_valid_name("abc'", false),
    
    test_valid_name("+", false),
    test_valid_name("abc+", false),
    test_valid_name("ab+cd", false),
    
    test_valid_name("#", false),
    test_valid_name("ab#", false),
    test_valid_name("ab#cd", false),
    
    test_create("", bad_topic),
    test_create("/", bad_topic),
    
    test_create("/a/b/c", topic),
    test_create("a/b/c", topic),
    
    test_create("a/b c", topic),
    test_create("a/b+c", bad_topic),
    test_create("a/b#c", bad_topic),
    test_create("a/b+", bad_topic),
    test_create("a/b#", bad_topic),
    
    test_create("a/b!c", topic),
    test_create("a/b'c", bad_topic),
    
    test_create("a//b/c", bad_topic),
    test_create("a/b/c/", bad_topic),
    test_create("a/b/c//", bad_topic),
    
    test_create("a/+/c", wildcard_topic),
    test_create("a/+/+", wildcard_topic),
    test_create("a/b/#", wildcard_topic),
    test_create("a/#/c", bad_topic),
    test_create("#", wildcard_topic),
    test_create("/#", wildcard_topic),
    
    %test_create( create("/a/b/c"), bad_topic),   % dialyzer will pick up this
    
    test_create( lists:concat(["/process/", io_lib:print(self()), "/control"]), topic ),
    
    test_create_list([], bad_topic),
    test_create_list([[]], bad_topic),
    
    test_create_list(["a", "b"], topic),
    test_create_list([[], "a", "b"], topic),
    
    test_create_list(["a", "#"], wildcard_topic),
    test_create_list(["a", "#", "b"], bad_topic),
    
    test_match("a/b/c", "a/b/c", true),
    test_match("a/b/c", "a/b/d", false),
    test_match("a/b/c", "/a/b/c", false),
    
    test_match("a/b/c", "a/+/c", true),
    test_match("a/b/c", "a/b/+", true),
    test_match("a/+/c", "a/b/c", true),
    test_match("a/+/c", "a/+/+", undefined),
    test_match("a/b/c", "a/+/+", true),
    test_match("a/b/c/d", "a/+/+", false),
    
    test_match("/a", "+/+", true),
    test_match("/a", "/+", true),
    test_match("/a", "+", false),
    
    test_match("a", "#", true),
    test_match("/a", "#", true),
    test_match("a", "/#", false),
    test_match("/a", "/#", true),
    
    test_match("a/b/c", "a/b/#", true),
    test_match("a/b", "a/b/#", true),
    test_match("a/b/c", "a/#", true),
    test_match("a/b/c", "a/+/#", true),
    
    ok.
        
