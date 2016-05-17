import sys
import hpx

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


def domain_low_bound(which):
	return locality_parameters['domain_size']//locality_parameters['domain_count']*which

def domain_high_bound(which):
	return locality_parameters['domain_size']//locality_parameters['domain_count']*(which+1)

def map_bounds_to_locality(low, high):
	domain_span = locality_parameters['domain_size']//locality_parameters['domain_count']
	low_idx = low//domain_span
	high_idx = high//domain_span
	assert high_idx >= low_idx
	if high_idx == low_idx:
		return low_idx
	elif high_idx - low_idx == 1:
		delta_low = domain_low_bound(high_idx) - low
		delta_high = high - domain_high_bound(low_idx)
		return (high_idx if delta_high > delta_low else low_idx)
	else:
		return low_idx


def print_usage(prog):
	print('Usage: %s <N parts> <Partition Limit> <theta> <domain size>\n' % (prog), file=sys.stderr)


if __name__ == "__main__":
	main(sys.argv)
