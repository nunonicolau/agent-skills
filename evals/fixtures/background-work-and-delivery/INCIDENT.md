# Double charge on redelivery

Order `order-9` was charged $5.00 twice. The broker delivered message `d1`, `provider.charge` succeeded, and the process crashed before ack. The dedupe map died with the process. The broker redelivered the same intent as `d2`, the map missed, and the provider charged again.

Two overlapping deliveries of `order-9` both read "not seen" before either write, so both charged.

A permanent provider rejection throws before ack, so the broker redelivers it with no cap and no dead-letter. A timeout throws the same way. Nothing records the attempt as unknown, so the next delivery calls the provider again instead of reconciling `order-9`.

The same intent id with a different payload does not reject. The hash check misses and the code charges the new body.

The checked-in test sends one delivery and passes.
