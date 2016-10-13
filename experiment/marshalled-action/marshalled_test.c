#include <hpx/hpx.h>
#include <stdio.h>
#include <time.h>

hpx_action_t main_action;
hpx_action_t marshalled_action;
hpx_action_t fixed_size_action;

struct send_buf
{
    int intbuff;
    long start_time;
};
typedef struct send_buf send_buf; 

int main_handler()
{
    struct timespec start_time;

    int intbuff = 23333;
    for(int i = 0; i < 50; i++) {
        clock_gettime(CLOCK_REALTIME, &start_time);
        hpx_call_sync(HPX_HERE, fixed_size_action, NULL, 0, &intbuff, &(start_time.tv_nsec));
    }

    for(int i = 0; i < 50; i++) {
        clock_gettime(CLOCK_REALTIME, &start_time);
        send_buf buf = {23333, start_time.tv_nsec};
        hpx_call_sync(HPX_HERE, marshalled_action, NULL, 0, &buf, sizeof(send_buf));
    }

    hpx_exit(HPX_SUCCESS);
}
HPX_ACTION(HPX_DEFAULT, HPX_ATTR_NONE, main_action, main_handler);

int marshalled_handler(void* args, size_t size)
{
    struct timespec end_time;
    clock_gettime(CLOCK_REALTIME, &end_time);
    send_buf* recv = (send_buf*) args;
    fprintf(stderr, "marshalled action takes: %ld ns.\n", end_time.tv_nsec - (recv->start_time));
    return HPX_SUCCESS;
}
HPX_ACTION(HPX_DEFAULT, HPX_MARSHALLED, marshalled_action, marshalled_handler,
           HPX_POINTER, HPX_SIZE_T);

int fixed_size_handler(int intbuff, long start_time)
{
    struct timespec end_time;
    clock_gettime(CLOCK_REALTIME, &end_time);
    fprintf(stderr, "fixed size action takes: %ld ns.\n", 
            end_time.tv_nsec - start_time);
    return HPX_SUCCESS;
}
HPX_ACTION(HPX_DEFAULT, HPX_ATTR_NONE, fixed_size_action, fixed_size_handler,
        HPX_INT, HPX_LONG);

int main()
{
    hpx_init(NULL, NULL);
    hpx_run(&main_action);
    hpx_finalize();
    return 0;
}
