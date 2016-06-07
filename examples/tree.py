import sys
import hpx
import numpy as np

particle_type = np.dtype([('pos', np.float64), ('mass', np.float64), ('phi', np.float64)])
moment_type = np.dtype([('mtot', np.float64), ('xcom', np.float64), ('Q00', np.float64)])
node_type = np.dtype([('left', hpx.get_numpy_type('hpx_addr_t')),
                      ('right', hpx.get_numpy_type('hpx_addr_t')),
                      ('low', np.float64),
                      ('high', np.float64),
                      ('moments', moment_type),
                      ('parts', hpx.get_numpy_type('hpx_addr_t')),
                      ('count', np.int32)])

locality_parameters = {}

def main(argv):
	try:
        hpx.init(argv)
	except RuntimeError:
		hpx.print_help()
		return

	if len(argv) != 5:
		print_usage(argv[0])
		return

	n_parts = int(argv[1])
	n_partition = int(argv[2])
	theta_c = float(argv[3])
	domain_size = float(argv[4])

	hpx.run(tree_main_action, n_parts, n_partition, theta_c, domain_size)

	hpx.finalize()


def tree_main(n_parts, n_partition, theta_c, domain_size):
	broadcast_domain_size(domain_size)
	
	root = create_node(0.0, domain_size)
	parts = generate_parts(n_parts, domain_size, root)
	
	partition_node_sync(root, parts, n_parts, n_partition)

	hpx.exit(hpx.SUCCESS)

tree_main_action = hpx.register_action(tree_main, 
									   hpx.DEFAULT,
									   hpx.ATTR_NONE, 
									   b'tree_action_key', 
									   [hpx.INT, hpx.INT, hpx.DOUBLE, hpx.DOUBLE])


def broadcast_domain_size(domain_size):
	hpx.bcast_rsync(broadcast_domain_action, domain_size)


def broadcast_domain_handler(size):
	locality_parameters['domain_size'] = size
	locality_parameters['domain_count'] = hpx.get_num_ranks()
	return hpx.SUCCESS

broadcast_domain_action = hpx.register_action(broadcast_domain_handler, 
											  hpx.DEFAULT, 
											  hpx.ATTR_NONE, 
											  b'broadcast_key',
											  [hpx.DOUBLE])


def partition_node_handler(node, parts, n_parts, n_partition):
	return 0

partition_node_action = hpx.register_action(partition_node_handler,
											hpx.DEFAULT,
											hpx.PINNED,
											b'partition_node_key',
											[hpx.POINTER, hpx.ADDR, hpx.INT, hpx.INT])

def domain_low_bound(which):
	return locality_parameters['domain_size']//locality_parameters['domain_count']*which

def domain_high_bound(which):
	return locality_parameters['domain_size']//locality_parameters['domain_count']*(which+1)

def map_bounds_to_locality(low, high):
	domain_span = locality_parameters['domain_size']//locality_parameters['domain_count']
	low_idx = int(low//domain_span)
	high_idx = int(high//domain_span)
	assert high_idx >= low_idx
	if high_idx == low_idx:
		return low_idx
	elif high_idx - low_idx == 1:
		delta_low = domain_low_bound(high_idx) - low
		delta_high = high - domain_high_bound(low_idx)
		return (high_idx if delta_high > delta_low else low_idx)
	else:
		return low_idx

def create_node(low, high):
	where = map_bounds_to_locality(low, high)
	retval = hpx.gas_alloc_local_at_sync(1, node_type.itemsize, 0, hpx.locality_address(where))
	vals = np.array([(hpx.NULL, hpx.NULL, low, high, (0.0, 0.0, 0.0), hpx.NULL, 0)], dtype=node_type)
	pointer, read_only_flag = vals.__array_interface__['data']
	hpx.gas_memput_rsync(retval, pointer, node_type.itemsize)
	return retval

def generate_parts(n_parts, domain_length, where):
	parts_gas = hpx.gas_alloc_local_at_sync(1, particle_type.itemsize*n_parts, 0, where)
	assert parts_gas != hpx.NULL
	parts_buffer = hpx.addr2buffer(hpx.gas_try_pin(parts_gas, return_local=True), particle_type.itemsize*n_parts)
	parts = np.frombuffer(parts_buffer, dtype=particle_type)

	parts['pos'] = np.random.rand(n_parts) * domain_length
	parts['mass'] = np.random.rand(n_parts) * 0.9 + 0.1

	hpx.gas_unpin(parts_gas)
	return parts_gas

def partition_node_sync(node, parts, n_parts, n_partition):
	done = hpx.lco_future_new(moment_type.itemsize)
	assert done != hpx.NULL
	hpx.call(node, partition_node_action, done, parts, n_parts, n_partition)
	hpx.lco_wait(done)
	hpx.lco_delete_sync(done)

def print_usage(prog):
	print('Usage: %s <N parts> <Partition Limit> <theta> <domain size>\n' % (prog), file=sys.stderr)


if __name__ == "__main__":
	main(sys.argv)
