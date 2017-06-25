#include <hpx/hpx.h>
#include <stdio.h>
#include <stdlib.h>

void calculate(int num);

hpx_action_t main_action, calculate_action;

int calculate_handler(int num)
{
	calculate(num);
	return HPX_SUCCESS;
}
HPX_ACTION(HPX_DEFAULT, HPX_ATTR_NONE, calculate_action, calculate_handler, HPX_INT);

int main_handler(int num_action)
{
	hpx_time_t start = hpx_time_now();

	hpx_addr_t and_lco = hpx_lco_and_new(num_action);
	for(int i = 0; i < num_action; i++) {
		int num = 5765760/num_action;
		hpx_call(HPX_HERE, calculate_action, and_lco, &num);
	}
	hpx_lco_wait(and_lco);

	printf("%lf\n", hpx_time_elapsed_ms(start));
	hpx_exit(0, NULL);
}
HPX_ACTION(HPX_DEFAULT, HPX_ATTR_NONE, main_action, main_handler, HPX_INT);

int main(int argc, char* argv[]) 
{
	hpx_init(&argc, &argv);
	int num_action = atoi(argv[1]);
	hpx_run(&main_action, NULL, &num_action);
	hpx_finalize();
	return 0;
}