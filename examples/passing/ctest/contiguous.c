#include <hpx/hpx.h>
#include <stdio.h>
#include <stdlib.h>
#define BLOCK_SIZE (1<<30)
#define NUM_TRY 10

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
    void* chunk_memory = malloc(BLOCK_SIZE/NUM_TRY);
    hpx_addr_t cc = hpx_thread_current_cont_target();
    hpx_addr_t buffer_completed = hpx_lco_future_new(0);
    hpx_call_async(HPX_THERE(target_rank), job_action, buffer_completed, cc, chunk_memory, BLOCK_SIZE/NUM_TRY);
    hpx_lco_wait(buffer_completed);
    return HPX_SUCCESS;
}
static HPX_ACTION(HPX_DEFAULT, 0, worker_action, worker_action_handler, HPX_INT, HPX_INT);

static int main_action_handler()
{
    hpx_time_t start_time = hpx_time_now();
    int total_rank = hpx_get_num_ranks();
    hpx_addr_t and_lco = hpx_lco_and_new(total_rank*NUM_TRY);
    for(int i = 0; i < total_rank; i++) {
        for(int j = 0; j < NUM_TRY; j++)
            hpx_call(HPX_THERE(i), worker_action, and_lco, &i, &total_rank);
    }
    hpx_lco_wait(and_lco);
    printf("%lf\n", (double)BLOCK_SIZE*total_rank/(1e-3*hpx_time_elapsed_ms(start_time))/1e9);
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
