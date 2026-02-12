"""
This is a fork of the INTERSECT control plane logic, slightly modified.

TODO - move everything in here to a new common library.

Why is this necessary? The SDK's design is heavily based around microservices and clients NEVER using wildcard characters in their topics/routing-keys.

Since we want to listen to ALL arbitrary messages, we need to use wildcards.
"""
