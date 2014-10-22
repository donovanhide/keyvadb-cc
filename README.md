#KeyvaDB


##Values File
```
uint64_t Length of length + key length + value length
key_type Key
string   Value
... repeats
```

##Keys file
```
uint32_t Level
key_type First key
key_type Last key
	key_type Key
	uint64_t Value file offset
	... repeats
	uint64_t Child node id
	... repeats
... repeats
```

##Journal File

Compressed node format:
```
uint64_t Node Id
uint32_t Level
key_type First key
key_type Last key
uint32_t Number of non-empty key values
uint32_t Number of non-empty children
	uint32_t key index
	key_type Key #0
	uint64_t Value #0
	... repeats
	uint32_t child index
	uint64_t Child #0
	... repeats
```

File format:
```
uint64_t Expected length of journal file
uint64_t Previous keys file length
uint64_t Previous values file length
uint32_t Number of changed nodes
Compressed node #0
Compressed node #1
.... repeats
```

##Commit Process

* Database has two buffers, active and commit.
* All puts are written to active under a lock.
* All gets check both active and commit under a lock before trying the key index tree.
* Flush thread swaps active and commit buffers under a lock.
* Flush thread now can access commit buffer for reads without a lock, no other thread will write to it.
* For each item in commit buffer:
	* If item does not already exist:
 		* Assign value file offset.
 		* Store changed and original nodes in memory.
* Create journal file.
* Write all keys and values to values file at assigned offsets (writev or pwrite).
* Write changed nodes to keys file.
* Delete journal.

##Recovery Process
* If journal file exists and expected length == actual length
	* Truncate values file to previous length
	* Truncate keys file to previous length
	* Write all original nodes to keys file
* Delete journal