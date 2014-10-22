#KeyvaDB

##Commit Process

Database has two buffers, active and commit.
All puts are written to active under a lock.
All gets check both active and commit under a lock before trying the key index tree.
Flush thread swaps active and commit buffers under a lock.
Flush thread now can access commit buffer for reads without a lock, no other thread will write to it.
For each item in commit buffer:
	If item does not already exist:
		Assign value file offset.
		Store changed and original nodes in memory.
Write all keys and values to values file at assigned offsets (writev or pwrite).
Write all key file length and original nodes to on-disk journal, storing only indexed non-empty key values to save disk space.
Write changed nodes to keys file.
Delete journal.
Write new value file length to value file header.

##Recovery Process

If value file length does not equal length in value file header, truncate to length in value file header.
If journal file exists write all original nodes to keys file and truncate to previous key file length and delete journal.