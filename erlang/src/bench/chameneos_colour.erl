-module(chameneos_colour).

-export([for_id/1, complement/2]).

-define(RED, red).
-define(YELLOW, yellow).
-define(BLUE, blue).
-define(FADED, faded).

-type chameneos_colour() :: ?RED | ?YELLOW | ?BLUE | ?FADED.

-export_type([chameneos_colour/0]).

-spec for_id(I :: integer()) -> chameneos_colour().
for_id(I) ->
	case I rem 3 of
		0 -> ?RED;
		1 -> ?YELLOW;
		2 -> ?BLUE
	end.

-spec complement(C1 :: chameneos_colour(), C2 :: chameneos_colour()) -> chameneos_colour().
complement(?RED, ?RED) -> ?RED;
complement(?RED, ?YELLOW) -> ?BLUE;
complement(?RED, ?BLUE) -> ?YELLOW;
complement(?YELLOW, ?RED) -> ?BLUE;
complement(?YELLOW, ?YELLOW) -> ?YELLOW;
complement(?YELLOW, ?BLUE) -> ?RED;
complement(?BLUE, ?RED) -> ?YELLOW;
complement(?BLUE, ?YELLOW) -> ?RED;
complement(?BLUE, ?BLUE) -> ?BLUE;
complement(_, ?FADED) -> ?FADED;
complement(?FADED, _) -> ?FADED.
