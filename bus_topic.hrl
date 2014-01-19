

%%% Different Topic types %%%

-record(bad_topic,
    {
        reason :: string()
    } ).
    
-record(topic,
    {
        parts :: list( string() )
    } ).
    
-record(wildcard_topic,
    {
        parts :: list( string() )
    } ).


-type all_topics_type() :: #bad_topic{} | #topic{} | #wildcard_topic{}.

