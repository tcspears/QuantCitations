# Todo for QuantCites

## Data Handling
- Change the DataCache so that repec/citec files are stored in nested directories. 
This should keep the cache from running into performance limits as the number of files grows. 
This can be implemented using the instructions described [here](https://stackoverflow.com/questions/273192/how-can-i-safely-create-a-nested-directory).
- Develop logging functionality (using logging package) so that the program's output is saved to 
a log in addition to being printed on screen.
- Make link_count increment after adding to the queue, not after writing article to DB. This
is needed to ensure that there won't be citing articles without corresponding article entries.
- Consider implementing persistence for the spidering queue and the visited_handles objects.
This would ensure that the whole database would not need to be rebuilt every time the algorithm
is re-run.

## Graph Implementation
- Read about connected component detection algorithms for DAGs
    - One approach would be to keep track of the network structure during the spidering process
    itself, and then store the path between cited_article and citing_article as a separate
    field. Then to get all articles 'connected' to a given article, we just have to select all
    entries that match a given cited_article and return all unique citing_articles.
- Read about how to implement DAGs in a SQL database (via a closure table?)
