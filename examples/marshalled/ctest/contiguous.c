#include <hpx/hpx.h>
#include <stdio.h>
#include <stdlib.h>

static HPX_ACTION_DECL(job_action);
static HPX_ACTION_DECL(worker_action);
static HPX_ACTION_DECL(main_action);

static int job_action_handler(void *buf, size_t n) 
{
	return HPX_SUCCESS;
}
static HPX_ACTION(HPX_DEFAULT, HPX_MARSHALLED, job_action, job_action_handler, HPX_POINTER, HPX_SIZE_T);

static int worker_action_handler(int current_rank, int num_rank)
{
	int target_rank = (current_rank + 1) % num_rank;
	void* chunk_memory = malloc(1024000000);
	hpx_call_cc(HPX_THERE(target_rank), job_action, chunk_memory, 1024000000);
	return HPX_SUCCESS;
}
static HPX_ACTION(HPX_DEFAULT, 0, worker_action, worker_action_handler, HPX_INT, HPX_INT);

static int main_action_handler()
{
	hpx_time_t start_time = hpx_time_now();
	int total_rank = hpx_get_num_ranks();
	hpx_addr_t and_lco = hpx_lco_and_new(total_rank);
	for(int i = 0; i < total_rank; i++) {
		hpx_call(HPX_THERE(i), worker_action, and_lco, &i, &total_rank);
	}
	hpx_lco_wait(and_lco);
	printf("%lf\n", hpx_time_elapsed_ms(start_time));
	hpx_exit(0, NULL);
}
static HPX_ACTION(HPX_DEFAULT, 0, main_action, main_action_handler);

int main(int argc, char *argv[argc])
{
	if (hpx_init(&argc, &argv) != 0)
    	return -1;
    hpx_run(&main_action, NULL);
    hpx_finalize();
	return 0;
}