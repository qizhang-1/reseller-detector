# Reseller Detector
## Purpose
This project is to design an end-to-end data pipe line and detection aglorithm for resellers for e-commerce website (e.g. amazon.com) via the transaction records.

## Modeling
Any 2 transaction ids are compared to check if shared information is used when the purchases are made.  If so, the 2 user ids are considered connected and a relation between them is established.  Once all the transaction records are compared, an undirected graph can be built.


## Algorithms
1. Compare the transaction records between any two ids
2. Build an undirected graph
3. Calculate the size of each connected components


## Data Pipeline
The data pipline is quite straightforward and the figure is self explanatory.    Transaction data store in csv files are piped into Spark nad GraphX for computing the connected components of the graph in the Undirected graph.  Detected user ids are stored in a relational database.
![Image of pipeline]
(https://github.com/qizhang-1/reseller-detector/blob/master/pipeline.png)

