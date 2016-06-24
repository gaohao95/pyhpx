N_parts=500
Partition_Limit=10
theta=0.3
domain_size=1000.0
rank=50
mpirun -n $rank --hostfile hosts -x PYTHONPATH -x PATH -x LD_LIBRARY_PATH python3 tree.py $N_parts $Partition_Limit $theta $domain_size
