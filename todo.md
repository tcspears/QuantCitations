# Todo for QuantCites

## Refactoring
- [ ] Reduce duplication in DataCache class between request_citec and request_repec

## Data Handling
- [X] Change the DataCache so that repec/citec files are stored in nested directories. 
This should keep the cache from running into performance limits as the number of files grows. 
This can be implemented using the instructions described 
[here](https://stackoverflow.com/questions/273192/how-can-i-safely-create-a-nested-directory).
- [X] Develop logging functionality (using logging package) so that the program's output is saved to 
a log in addition to being printed on screen.
- [X] Make link_count increment after adding to the queue, not after writing article to DB. This
is needed to ensure that there won't be citing articles without corresponding article entries.
- [X] Implement persistence for the spidering queue and the visited_handles objects.
This would ensure that the whole database would not need to be rebuilt every time the algorithm
is re-run.

## Graph Implementation
- [X] Implement citation network representation using postgres ltrees. 