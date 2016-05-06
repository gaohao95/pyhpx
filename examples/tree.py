import sys
import hpx

def main(argv):
	
	tree_main_action = hpx.register_action(tree_main, 
										   hpx.DEFAULT,
										   hpx.ATTR_NONE, 
										   b'tree_action_key', 
										   [hpx.INT, hpx.INT, hpx.DOUBLE, hpx.DOUBLE])

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
	hpx.exit(hpx.SUCCESS)

def print_usage(prog):
	print('Usage: %s <N parts> <Partition Limit> <theta> <domain size>\n' % (prog), file=sys.stderr)

if __name__ == "__main__":
	main(sys.argv)