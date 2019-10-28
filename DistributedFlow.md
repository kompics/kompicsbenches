1) Script starts Master
2) Script starts N-1 Clients pointing them to Master
3) Script starts Runner
4) Client check in with Master during INIT phase (move to setup once all have checked in)
5) Runner initiates experiment on Master
6) Master sends run configuration during setup phase and waits for all responses
7) Master executes run
8) Master sends cleanup to all clients and waits for all responses
Repeat 6-8 until end of runs
9) Master responds with results to Runner
Repeat 5-9 for all experiments
10) Runner shuts down
11) Script terminates Master and all Clients