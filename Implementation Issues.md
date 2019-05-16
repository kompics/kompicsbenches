Implementation Issues
=====================


Actix
-----
https://actix.rs/

- No proper threadpools (router-style single actor type only)
- No dispatching (low-level tokio direct networking only)

Riker
-----
https://github.com/riker-rs/riker/

- Not networking implementation
