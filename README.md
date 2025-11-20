# json-register
A Rust version of our existing json register for integration into rust apps


* Pure Rust but with PYO3/Maturin wrapper that replicates the existing library in py-json-register
* Uses LRU cache, with canonicalised JSON as keys
* Uses the same Postgres data model as py-json-register

