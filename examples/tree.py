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
	locality_parameters['domain_size'] = sizebbs
	locality_parameters['domain_count'] = hpx.get_num_ranks()

broadcast_domain_action = hpx.register_action(broadcast_domain_handler, 
											  hpx.DEFAULT, 
											  hpx.ATTR_NONE, 
											  b'broadcast_key',
											  [hpx.DOUBLE])


def print_usage(prog):
	print('Usage: %s <N parts> <Partition Limit> <theta> <domain size>\n' % (prog), file=sys.stderr)


if __name__ == "__main__":
	main(sys.argv)