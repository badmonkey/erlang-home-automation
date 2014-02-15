

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
-type valid_topic_type() :: #topic{} | #wildcard_topic{}.


-type bus_run_modes() :: mode_startup | mode_running .


