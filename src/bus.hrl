

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

-type return_type() :: ok | { error, string() }.


-type bus_run_modes() :: mode_startup | mode_running .


-type bus_event_type() :: { node_has_listener, pid() }
						| { node_last_listener, pid() } .

-type bus_driver_type() :: fun( ( bus_event_type() ) -> return_type() ) | undefined.



