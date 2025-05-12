Hello! 

Here is the basic run-down of what is used within the aeron-samples folder for the Jepsen Aeron Cluster analysis for UIUC CS 598.

---
aeron-samples/scripts/cluster holds scripts that are used by the Jepsen code to setup and teardown the cluster namespaces (setup-namespaces, remove-namespaces), 
launch the cluster within the namespaces (basic-auction-cluster-ns), and launch the HTTP server wrapper that Jepsen uses to communication with the cluster (setup-http).

These four scripts are all used by the Jepsen code, and would only need to exist on your machine with the correct filepath to successfully launch the Jepsen test. 

---
aeron-samples/src/main/java/io/aeron/samples/cluster/tutorial holds the multi-item auction clustered service logic (BasicAuctionCluseredService), and the HTTP service wrapper 
implementation that launches Aeron Cluster clients to communicate with the cluster via HTTP calls (AuctionHttpServer)

These two files in particular, are used within the scripts described above to sucessfully launch the cluster and communication with the cluster.