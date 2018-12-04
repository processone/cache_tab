# Version 1.0.17

* Add ets\_cache:insert/3,4 functions

# Version 1.0.16

* Updating p1_utils to version 1.0.13.

# Version 1.0.15

* Updating p1_utils to version 6ff85e8.
* Don't compile ets\_cache to native code
* Use rand:unifor instead of crypto module

# Version 1.0.14

* Updating p1_utils to version 1.0.12.

# Version 1.0.13

* Updating p1_utils to version 1.0.11.
* Fix compilation with rebar3

# Version 1.0.12

* Treat {error, notfound} callback result as a lookup mismatch

# Version 1.0.11

* Updating p1_utils to version 1.0.10.

# Version 1.0.10

* Fix hex packaging

# Version 1.0.9

* Make sure we publish include dir on hex.pm

# Version 1.0.8

* Make rebar.config.script more structured and fix problem with coveralls
* Add new cache implementation on top of ETS
* Introduce lookup/2 insert/3,4 insert_new/3,4 calls
* Export clear/2
* Store ets_cache options globally
* Add start/0 and stop/0 functions for convenience
* Rewrite ets_cache module
* Fix info/1
* Don't crash if counter doesn't exist

# Version 1.0.7

* Use p1_utils v1.0.7

# Version 1.0.6

* Add dirty_dist_insert and dirty_dist_delete calls (Alexey Shchepin)

# Version 1.0.5

* Use p1_utils v1.0.6 (Christophe Romain)

# Version 1.0.4

* Use p1_utils v1.0.5 (Mickaël Rémond)

# Version 1.0.3

* Use p1_utils v1.0.4 (Mickaël Rémond)

# Version 1.0.2

* Use p1_utils v1.0.3 (Mickaël Rémond)
