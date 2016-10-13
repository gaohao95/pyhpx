#include <hpx/hpx.h>
#include <stdio.h>

hpx_action_t main_action;
hpx_action_t marshalled_action;
hpx_action_t fixed_size_action;

struct send_buf
{
    int intbuff;
    hpx_time_t start_time;
};
typedef struct send_buf send_buf; 

int main_handler()
{
    send_buf buf = {23333, hpx_time_now()};
    hpx_call_sync(HPX_HERE, marshalled_action, NULL, 0, &buf, sizeof(send_buf));
    
    int intbuff = 23333;
    hpx_time_t start_time = hpx_time_now();
    hpx_call_sync(HPX_HERE, fixed_size_action, NULL, 0, &intbuff, &start_time);

    hpx_exit(HPX_SUCCESS);
}
HPX_ACTION(HPX_DEFAULT, HPX_ATTR_NONE, main_action, main_handler);

int marshalled_handler(void* args, size_t size)
{
    hpx_time_t end_time = hpx_time_now();
    send_buf* recv = (send_buf*) args;
    fprintf(stderr, "marshalled action takes: %lld ns.\n", hpx_time_diff_ns(recv -> start_time, end_time));
    return HPX_SUCCESS;
}
HPX_ACTION(HPX_DEFAULT, HPX_MARSHALLED, marshalled_action, marshalled_handler,
           HPX_POINTER, HPX_SIZE_T);

int fixed_size_handler(int intbuff, hpx_time_t start_time)
{
    hpx_time_t end_time = hpx_time_now();
    fprintf(stderr, "fixed size action takes: %lld ns.\n", hpx_time_diff_ns(start_time, end_time));
    return HPX_SUCCESS;
}
HPX_ACTION(HPX_DEFAULT, HPX_ATTR_NONE, fixed_size_action, fixed_size_handler,
        HPX_INT, HPX_UINT64);

int main()
{
    hpx_init(NULL, NULL);
    hpx_run(&main_action);
    hpx_finalize();
    return 0;
}
